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

        foreach ($options as $key => $value) {
            if (!isset($this->options[$key])) {
                throw new Exception\UnknownOption($key);
            }

            $this->options[$key] = $value;
        }
    }

    abstract protected function setDefaultOptions();

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

}
