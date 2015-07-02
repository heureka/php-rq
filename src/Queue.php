<?php

namespace PhpRQ;

use Predis\Collection\Iterator\HashKey;
use PhpRQ\Exception\InvalidArgument;

/**
 * Queue is a simple queue implemented using List as the queue, multiple Lists as the processing queues and a Hash as
 * a storage for processing queue timeouts (so you can tell which processing queue has expired).
 * There is no priority whatsoever - items are processed as they were inserted into the queue.
 *
 * Queue needs a garbage collector process because the queue creates a processing queues every time you request items
 * from it. This process is implemented by the methods reEnqueue* and drop* of this class and they should be called
 * before getting the items or periodically (if you don't care about the order of the items).
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Queue extends Base
{

    const OPT_ADD_MAX_CHUNK_SIZE        = 0;
    const OPT_GET_MAX_CHUNK_SIZE        = 1;
    const OPT_DEL_MAX_CHUNK_SIZE        = 2;
    const OPT_PROCESSING_SUFFIX         = 3;
    const OPT_PROCESSING_TIMEOUT        = 4;
    const OPT_PROCESSING_TIMEOUT_SUFFIX = 5;

    /**
     * Queue name
     *
     * @var string
     */
    private $queue;

    private $options = [
        self::OPT_ADD_MAX_CHUNK_SIZE        => 100,           // items count
        self::OPT_GET_MAX_CHUNK_SIZE        => 100,           // items count
        self::OPT_DEL_MAX_CHUNK_SIZE        => 1000,          // items count
        self::OPT_PROCESSING_SUFFIX         => '-processing', // string suffix
        self::OPT_PROCESSING_TIMEOUT        => 7200,          // seconds
        self::OPT_PROCESSING_TIMEOUT_SUFFIX => '-timeouts',   // string suffix
    ];

    /**
     * @var string
     */
    private $clientID;

    /**
     * @param ClientInterface $redis
     * @param string          $queue   Queue name
     * @param array           $options
     *
     * @throws Exception\UnknownOption
     */
    public function __construct(ClientInterface $redis, $queue, $options = [])
    {
        parent::__construct($redis);
        $this->queue = $queue;
        foreach ($options as $key => $value) {
            if (!isset($this->options[$key])) {
                throw new Exception\UnknownOption($key);
            }

            $this->options[$key] = $value;
        }
        $this->clientID = sprintf('%s[%d][%d]', gethostname(), getmypid(), time());
    }

    /**
     * Returns the Queue name
     *
     * @return string
     */
    public function getQueueName()
    {
        return $this->queue;
    }

    /**
     * Returns the number of items in the queue
     *
     * @return int
     */
    public function getCount()
    {
        return $this->redis->llen($this->queue);
    }

    /**
     * Adds a new item to the queue
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

        $this->redis->lpush($this->queue, $item);
    }

    /**
     * Adds new items to the queue
     *
     * @param array $items Array of values which can be converted to string
     *
     * @throws Exception\InvalidArgument
     */
    public function addItems(array $items)
    {
        $pipe = $this->redis->pipeline();
        foreach (array_chunk($items, $this->options[self::OPT_ADD_MAX_CHUNK_SIZE]) as $chunk) {
            foreach ($chunk as $item) {
                $item = (string)$item;
                if (empty($item)) {
                    throw new Exception\InvalidArgument('Items in $items mustn\'t be empty');
                }
            }
            $pipe->lpush($this->queue, $chunk);
        }
        $pipe->execute();
    }

    /**
     * Returns certain amount of items from the queue
     *
     * @param int $size Number of items to return
     *
     * @return array
     */
    public function getItems($size)
    {
        if (!is_int($size) || $size < 1) {
            throw new InvalidArgument('Size must be an integer larger than zero.');
        }

        $steps = [];
        $max = $this->options[self::OPT_GET_MAX_CHUNK_SIZE];
        for ( ; $size >= $max; $size -= $max) {
            $steps[] = $max;
        }

        if ($size) {
            $steps[] = $size;
        }

        $processingQueueName = $this->getProcessingQueueName();
        $timeoutsHashName = $this->getTimeoutsHashName();

        $result = [];
        foreach ($steps as $size) {
            $chunk = $this->redis->queueGet(
                $this->queue,
                $processingQueueName,
                $timeoutsHashName,
                $size,
                microtime(true)
            );

            $result = array_merge($result, $chunk);
            if (count($chunk) < $size) {
                break;
            }
        }

        return $result;
    }

    /**
     * Returns all the items from the queue
     *
     * @return array
     */
    public function getAllItems()
    {
        $processingQueueName = $this->getProcessingQueueName();
        $timeoutsHashName = $this->getTimeoutsHashName();

        $result = [];
        while (true) {
            $chunk = $this->redis->queueGet(
                $this->queue,
                $processingQueueName,
                $timeoutsHashName,
                $this->options[self::OPT_GET_MAX_CHUNK_SIZE],
                microtime(true)
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
        $this->redis->queueAck($this->getProcessingQueueName(), $this->getTimeoutsHashName(), (string)$item);
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
        try {
            $processingQueueName = $this->getProcessingQueueName();
            $timeoutsHashName = $this->getTimeoutsHashName();

            $pipe = $this->redis->pipeline();
            foreach ($items as $item) {
                $pipe->queueAck($processingQueueName, $timeoutsHashName, (string)$item);
            }
            $pipe->execute();
        } catch (\Predis\Response\ServerException $e) {
            if ($e->getErrorType() === 'NOSCRIPT') {
                // this may happen once, when the script isn't loaded into server cache
                // the following code will guarantee that the script is loaded and that this won't happen again
                $first = array_shift($items);
                $this->ackItem((string)$first);
                if ($items) {
                    $this->ackItems($items);
                }

                return;
            }

            throw $e;
        }
    }

    /**
     * Rejects an item that wasn't processed correctly (puts it back on the head of the queue)
     *
     * Be aware that if you use this method then you lose the order of the items in the queue
     * (among the items of the current batch). If you want to preserve the order of the items then use
     * method Queue::rejectBatch to re-enqueue all the remaining items in the current batch.
     *
     * @param mixed $item
     */
    public function rejectItem($item)
    {
        $this->redis->queueReject(
            $this->queue,
            $this->getProcessingQueueName(),
            $this->getTimeoutsHashName(),
            (string)$item
        );
    }

    /**
     * Rejects items that weren't processed correctly (puts them back on the head of the queue)
     *
     * Be aware that if you use this method then you may lose the order of the items in the queue
     * (in among of the items in the current batch). If you want to preserve the order of the items then use
     * method Queue::rejectBatch to re-enqueue all the remaining items in the current batch.
     *
     * @param array $items
     *
     * @throws \Exception
     */
    public function rejectItems(array $items)
    {
        try {
            $reversedItems = array_reverse($items);

            $processingQueueName = $this->getProcessingQueueName();
            $timeoutsHashName = $this->getTimeoutsHashName();

            $pipe = $this->redis->pipeline();
            foreach ($reversedItems as $item) {
                $pipe->queueReject($this->queue, $processingQueueName, $timeoutsHashName, (string)$item);
            }
            $pipe->execute();
        } catch (\Predis\Response\ServerException $e) {
            if ($e->getErrorType() === 'NOSCRIPT') {
                // this may happen once, when the script isn't loaded into server cache
                // the following code will guarantee that the script is loaded and that this won't happen again
                $first = array_pop($items);
                $this->rejectItem((string)$first);
                if ($items) {
                    $this->rejectItems($items);
                }

                return;
            }

            throw $e;
        }
    }

    /**
     * Rejects the items that are currently being processed (puts them back on the head of the queue)
     *
     * Use this method if you want to re-enqueue all the remaining items in the current batch to preserve theirs order
     */
    public function rejectBatch()
    {
        $this->redis->queueReEnqueue($this->queue, $this->getProcessingQueueName(), $this->getTimeoutsHashName());
    }

    /**
     * Puts all the timed out items from all the processing lists back on the head of the queue.
     *
     * If you use single consumer then check method Queue::reEnqueueAllItems
     * This method should be called periodically to check if there are any failed tasks
     * Alternatively you can use Queue:::dropTimedOutItems to drop the items instead of re-enqueing them
     *
     * @param int|null $timeout If null is passed then the value from options is used
     */
    public function reEnqueueTimedOutItems($timeout = null)
    {
        if (!is_int($timeout) && $timeout !== null) {
            throw new InvalidArgument('$timeout must be an integer or null (to value from the options)');
        }

        if ($timeout === null) {
            $timeout = $this->options[self::OPT_PROCESSING_TIMEOUT];
        }

        $timeoutsHashName = $this->getTimeoutsHashName();

        $queues = iterator_to_array(new HashKey($this->redis, $timeoutsHashName));
        arsort($queues, SORT_NUMERIC);
        foreach ($queues as $processingQueueName => $time) {
            if ($time + $timeout < time()) {
                $this->redis->queueReEnqueue($this->queue, $processingQueueName, $timeoutsHashName);
            }
        }
    }

    /**
     * Puts all the items from all the processing lists back on the head of the queue.
     *
     * If you are getting items from the queue from a single place (one consumer) then you can use this
     * method to re-enqueue all the failed items without checking the processing timeout before getting new items.
     * Alternatively you can use Queue:::dropAllItems to drop items instead of re-enqueing them
     */
    public function reEnqueueAllItems()
    {
        $timeoutsHashName = $this->getTimeoutsHashName();

        $queues = iterator_to_array(new HashKey($this->redis, $timeoutsHashName));
        arsort($queues, SORT_NUMERIC);
        foreach ($queues as $processingQueueName => $time) {
            $this->redis->queueReEnqueue($this->queue, $processingQueueName, $timeoutsHashName);
        }
    }

    /**
     * Drops all the timed out items from all the processing lists
     *
     * If you use single consumer then check method Queue::dropAllItems
     * This method should be called periodically to check if there are any failed tasks and drop them
     * Alternatively you can use Queue:::reEnqueueTimedOutItems to re-enqueue items instead of dropping them
     *
     * @param int|null $timeout If null is passed then the value from options is used
     */
    public function dropTimedOutItems($timeout = null)
    {
        if (!is_int($timeout) && $timeout !== null) {
            throw new InvalidArgument('$timeout must be an integer or null (to value from the options)');
        }

        if ($timeout === null) {
            $timeout = $this->options[self::OPT_PROCESSING_TIMEOUT];
        }

        $timeoutsHashName = $this->getTimeoutsHashName();

        $queues = iterator_to_array(new HashKey($this->redis, $timeoutsHashName));
        $pipe = $this->redis->pipeline();
        foreach ($queues as $processingQueueName => $time) {
            if ($time + $timeout < time()) {
                $pipe->del($processingQueueName);
                $pipe->hdel($timeoutsHashName, $processingQueueName);
            }
        }
        $pipe->execute();
    }

    /**
     * Drops all the items from all the processing lists
     *
     * If you are getting items from the queue from a single place (one consumer) then you can use this
     * method to drop all failed items without checking the processing timeout.
     * Alternatively you can use Queue:::reEnqueueAllItems to re-enqueue items instead of dropping them
     */
    public function dropAllItems()
    {
        $timeoutsHashName = $this->getTimeoutsHashName();

        $queues = iterator_to_array(new HashKey($this->redis, $timeoutsHashName));
        $pipe = $this->redis->pipeline();
        foreach ($queues as $processingQueueName => $time) {
            $pipe->del($processingQueueName);
            $pipe->hdel($timeoutsHashName, $processingQueueName);
        }
        $pipe->execute();
    }

    /**
     * Clears all items from the queue. It also removes all the processing lists (regardless of the age).
     */
    public function clearQueue()
    {
        $this->dropAllItems();

        do {
            $pipe = $this->redis->pipeline();
            for ($i = 0; $i < $this->options[self::OPT_DEL_MAX_CHUNK_SIZE]; $i++) {
                $pipe->rpop($this->queue);
            }
            $pipe->execute();
        } while ($this->redis->rpop($this->queue) !== null);
    }

    /**
     * @return string
     */
    private function getProcessingQueueName()
    {
        $baseName = $this->queue . $this->options[self::OPT_PROCESSING_SUFFIX];

        return $baseName . '-' . $this->clientID;
    }

    /**
     * @return string
     */
    private function getTimeoutsHashName()
    {
        return $this->queue . $this->options[self::OPT_PROCESSING_TIMEOUT_SUFFIX];
    }

}
