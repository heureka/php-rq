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


    /** Microseconds to wait between retries. Will use exponential backoff @see the line 37 */
    const DEFAULT_RETRY_WAIT = 100;

    /** Maximal count of attempts */
    const DEFAULT_MAX_ATTEMPTS = 10;

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
     * @var Time
     */
    protected $time;

    /**
     * @param ClientInterface $redis
     * @param string          $name
     * @param array           $options
     * @param Time|null       $time
     *
     * @throws Exception\UnknownOption
     */
    public function __construct(ClientInterface $redis, $name, $options = [], Time $time = null)
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

        $this->time = $time ?: new Time();
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
     * Functional wrapper for retrying call of a PhpRQ method to avoid raising Predis\Connection\ConnectionException
     * in case of a packet loss.
     *
     * @param callable      $callback        Callback which will be executed.
     * @param callable|null $successCallback Called only if the callback call has been successful.
     * @param callable|null $failureCallback Called only if the callback call has failed
     * @param int           $maxAttempts     Maximal count of attempts.
     * @param int           $retryWait       Microseconds to wait between retries.
     *
     * @return mixed Return value from injected callable function
     *
     * @throws \Predis\Connection\ConnectionException
     */
    public function safeExecution(
        callable $callback,
        callable $successCallback = null,
        callable $failureCallback = null,
        $maxAttempts=self::DEFAULT_MAX_ATTEMPTS,
        $retryWait=self::DEFAULT_RETRY_WAIT
    ) {
        $retries = 0;
        do {
            try {
                $returnValue = call_user_func($callback, $this);
                if ($successCallback !== null) {
                    $successCallback($returnValue);
                }
                return $returnValue;
                break;
            } catch (\Predis\Connection\ConnectionException $exception) {
                if ($failureCallback !== null) {
                    $failureCallback();
                }
                usleep($retryWait * $retries ** 2);
            }

            $retries++;

        } while ($retries < $maxAttempts);

        throw $exception;
    }

}
