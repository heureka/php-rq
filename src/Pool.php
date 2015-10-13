<?php

namespace PhpRQ;

use PhpRQ\Exception\InvalidArgument;

/**
 * Pool is intended for "queues" which are most of the time the same - the items need to be processed periodically.
 * This is exactly how the pool works - it processes items that are "outdated". When the item is processed (ACKed),
 * the validity of the item is set accordingly to the options (e.g. for 36 hours as of default).
 *
 * There is no need for a garbage collector process - items that were failed to process are automatically processed
 * after the ACK_TTL time has passed.
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Pool extends Base
{

    const OPT_ADD_MAX_CHUNK_SIZE = 0;
    const OPT_GET_MAX_CHUNK_SIZE = 1;
    const OPT_ACK_MAX_CHUNK_SIZE = 2;
    const OPT_DEL_MAX_CHUNK_SIZE = 3;
    const OPT_ACK_TTL            = 4;
    const OPT_ACK_VALID_FOR      = 5;

    /**
     * @deprecated @see Base::getName()
     * @return string
     */
    public function getPoolName()
    {
        return $this->getName();
    }

    /**
     * Returns the number of items in the pool
     *
     * @return int
     */
    public function getCount()
    {
        return $this->redis->zcard($this->name);
    }

    /**
     * Returns the number of items in the pool which should be processed
     *
     * @return int
     */
    public function getCountToProcess()
    {
        return $this->redis->zcount($this->name, '-inf', $this->time->now());
    }

    /**
     * Checks if the given item is present in the pool (may be array of items)
     *
     * @param mixed|array $item Single item or array of items which can be converted to string
     *
     * @return bool|bool[] Array of booleans indexed by $item values if $item is an array, boolean if $item is scalar
     */
    public function isInPool($item)
    {
        if (is_array($item)) {
            $items = array_values($item);

            $pipe = $this->redis->pipeline();
            foreach ($items as $item) {
                $pipe->zscore($this->name, (string)$item);
            }

            $result = [];
            foreach ($pipe->execute() as $i => $item) {
                if ($item === null) {
                    $result[$items[$i]] = false;
                } else {
                    $result[$items[$i]] = true;
                }
            }

            return $result;
        } else {
            return $this->redis->zscore($this->name, $item) !== null;
        }
    }

    /**
     * Adds a new item to the pool
     *
     * @param mixed $item Value which can be converted to string
     *
     * @throws Exception\InvalidArgument
     */
    public function addItem($item)
    {
        $item = (string)$item;
        if (empty($item)) {
            throw new Exception\InvalidArgument('$item mustn\'t be empty');
        }

        $this->redis->zadd($this->name, $this->time->now(), $item);
        $this->waitForSlaveSync();
    }

    /**
     * Adds new items to the pool
     *
     * @param array $items Array of values which can be converted to string
     *
     * @throws Exception\InvalidArgument
     */
    public function addItems(array $items)
    {
        $time = $this->time->now();
        $itemsToAdd = [];
        foreach ($items as $item) {
            $item = (string)$item;
            if (empty($item)) {
                throw new Exception\InvalidArgument('Items in $items mustn\'t be empty');
            }

            $itemsToAdd[$item] = $time;
        }

        foreach (array_chunk($itemsToAdd, $this->options[self::OPT_ADD_MAX_CHUNK_SIZE], true) as $chunk) {
            $this->redis->zadd($this->name, $chunk);
        }

        $this->waitForSlaveSync();
    }

    /**
     * Returns certain amount of items from the pool
     *
     * @param int $size Number of items to return
     *
     * @return array
     */
    public function getItems($size)
    {
        if (!is_int($size) || $size < 1) {
            throw new InvalidArgument('$size must be an integer larger than zero.');
        }

        $steps = [];
        $max = $this->options[self::OPT_GET_MAX_CHUNK_SIZE];
        for ( ; $size >= $max; $size -= $max) {
            $steps[] = $max;
        }

        if ($size) {
            $steps[] = $size;
        }

        $result = [];
        foreach ($steps as $size) {
            $chunk = $this->redis->poolGet($this->name, $size, $this->time->now(), $this->options[self::OPT_ACK_TTL]);
            $result = array_merge($result, $chunk);
            if (count($chunk) < count($size)) {
                break;
            }
        }

        return $result;
    }

    /**
     * Returns all the items from the pool
     *
     * @return array
     */
    public function getAllItems()
    {
        $result = [];
        while (true) {
            $chunk = $this->redis->poolGet(
                $this->name,
                $this->options[self::OPT_GET_MAX_CHUNK_SIZE],
                $this->time->now(),
                $this->options[self::OPT_ACK_TTL]
            );
            $result = array_merge($result, $chunk);

            if (count($chunk) < $this->options[self::OPT_GET_MAX_CHUNK_SIZE]) {
                break;
            }
        }

        return $result;
    }

    /**
     * Acknowledges an item that was processed correctly
     *
     * @param mixed $item
     */
    public function ackItem($item)
    {
        $this->ackItemWithoutSync($item);
        $this->waitForSlaveSync();
    }

    /**
     * Acknowledges items that were processed correctly
     *
     * @param array $items
     *
     * @throws \Exception
     */
    public function ackItems(array $items)
    {
        foreach (array_chunk($items, $this->options[self::OPT_ACK_MAX_CHUNK_SIZE], true) as $chunk) {
            try {
                $pipe = $this->redis->pipeline();
                foreach ($chunk as $item) {
                    $pipe->poolAck($this->name, $item, $this->time->now() + $this->options[self::OPT_ACK_VALID_FOR]);
                }
                $pipe->execute();
                $this->waitForSlaveSync();
            } catch (\Predis\Response\ServerException $e) {
                if ($e->getErrorType() === 'NOSCRIPT') {
                    // this may happen once, when the script isn't loaded into server cache
                    // the following code will guarantee that the script is loaded and that this won't happen again
                    $first = array_shift($items);
                    $this->ackItemWithoutSync($first);
                    if ($items) {
                        $this->ackItems($items);
                        return;
                    }

                    $this->waitForSlaveSync();

                    return;
                }

                throw $e;
            }
        }
    }

    /**
     * Removes an item that is no longer valid
     *
     * @param mixed $item
     */
    public function removeItem($item)
    {
        $this->removeItemWithoutSync($item);
        $this->waitForSlaveSync();
    }

    /**
     * Removes items that are no longer valid
     *
     * @param array $items
     *
     * @throws \Exception
     */
    public function removeItems($items)
    {
        foreach (array_chunk($items, $this->options[self::OPT_DEL_MAX_CHUNK_SIZE], true) as $chunk) {
            try {
                $pipe = $this->redis->pipeline();
                foreach ($chunk as $item) {
                    $pipe->poolRemove($this->name, $item);
                }
                $pipe->execute();
                $this->waitForSlaveSync();
            } catch (\Predis\Response\ServerException $e) {
                if ($e->getErrorType() === 'NOSCRIPT') {
                    // this may happen once, when the script isn't loaded into server cache
                    // the following code will guarantee that the script is loaded and that this won't happen again
                    $first = array_shift($items);
                    $this->removeItemWithoutSync($first);
                    if ($items) {
                        $this->removeItems($items);
                        return;
                    }

                    $this->waitForSlaveSync();

                    return;
                }

                throw $e;
            }
        }
    }

    /**
     * Clears all the items from the pool.
     */
    public function clearPool()
    {
        do {
            $removed = $this->redis->zremrangebyrank($this->name, 0, $this->options[self::OPT_DEL_MAX_CHUNK_SIZE] - 1);
        } while ($removed !== 0);

        $this->waitForSlaveSync();
    }

    protected function setDefaultOptions()
    {
        $this->options = [
            self::OPT_ADD_MAX_CHUNK_SIZE => 100,    // items count
            self::OPT_GET_MAX_CHUNK_SIZE => 100,    // items count
            self::OPT_ACK_MAX_CHUNK_SIZE => 500,    // items count
            self::OPT_DEL_MAX_CHUNK_SIZE => 100,    // items count
            self::OPT_ACK_TTL            => 600,    // seconds
            self::OPT_ACK_VALID_FOR      => 129600, // seconds
        ];
    }

    /**
     * @param mixed $item
     */
    private function ackItemWithoutSync($item)
    {
        $this->redis->poolAck($this->name, $item, $this->time->now() + $this->options[self::OPT_ACK_VALID_FOR]);
    }

    /**
     * @param mixed $item
     */
    private function removeItemWithoutSync($item)
    {
        $this->redis->poolRemove($this->name, $item);
    }

}
