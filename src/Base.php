<?php

namespace PhpRQ;

/**
 * Base class for all the queue classes.
 * It takes care of the basic functionality like assigning/returning the Redis client, the queue name etc
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
abstract class Base
{

    const OPT_SLAVES_SYNC_ENABLED        = -1;
    const OPT_SLAVES_SYNC_REQUIRED_COUNT = -2;
    const OPT_SLAVES_SYNC_TIMEOUT        = -3;

    /**
     * @var ClientInterface
     */
    protected $redis;

    /**
     * Name of the queue
     *
     * @var string
     */
    protected $name;

    /**
     * Array of available options
     * Those options are set by derived classes and can be overridden by a constructor parameter
     *
     * @var array
     */
    protected $options;

    /**
     * For testing we want use DI defined timestamp
     * @var int|null timestamp
     */
    protected $timeForTesting = null;

    /**
     * @param ClientInterface $redis
     * @param string          $name
     * @param array           $options
     *
     * @throws Exception\UnknownOption
     */
    public function __construct(ClientInterface $redis, $name, $options = [])
    {
        $this->redis = $redis;
        $this->name  = $name;

        $this->setDefaultOptions();

        $this->options[self::OPT_SLAVES_SYNC_ENABLED]        = false;
        $this->options[self::OPT_SLAVES_SYNC_REQUIRED_COUNT] = 0;
        $this->options[self::OPT_SLAVES_SYNC_TIMEOUT]        = 100;

        foreach ($options as $key => $value) {
            if (!isset($this->options[$key])) {
                throw new Exception\UnknownOption($key);
            }

            $this->options[$key] = $value;
        }
    }

    abstract protected function setDefaultOptions();

    /**
     * If slave synchronous syncing is enabled (@see self::OPT_SLAVES_SYNC_ENABLED) then this method ensures
     * that @see self::OPT_SLAVES_SYNC_REQUIRED_COUNT number of slaves will acknowledge all issued commands
     * within @see self::OPT_SLAVES_SYNC_TIMEOUT. If syncing is enabled and slaves fail to acknowledge the issued
     * commands then an exception is thrown
     */
    protected function waitForSlaveSync()
    {
        if ($this->options[self::OPT_SLAVES_SYNC_ENABLED]) {
            $synced = $this->redis->wait(
                $this->options[self::OPT_SLAVES_SYNC_REQUIRED_COUNT],
                $this->options[self::OPT_SLAVES_SYNC_TIMEOUT]
            );

            if ($synced < $this->options[self::OPT_SLAVES_SYNC_REQUIRED_COUNT]) {
                throw new Exception\NotEnoughSlavesSynced(sprintf('Required: %d, synced: %d',
                    $this->options[self::OPT_SLAVES_SYNC_REQUIRED_COUNT], $synced
                ));
            }
        }
    }

    /**
     * Returns the Redis client (useful for disconnecting)
     *
     * @return ClientInterface
     */
    public function getRedisClient()
    {
        return $this->redis;
    }

    /**
     * Returns the queue's name
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param $timeStamp
     */
    public function setTimeForTestingPurpose($timeStamp)
    {
        $this->timeForTesting = $timeStamp;
    }

    /**
     * @return int
     */
    protected function getTime()
    {
        return null !== $this->timeForTesting ? $this->timeForTesting : time();
    }

}
