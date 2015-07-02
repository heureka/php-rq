<?php

namespace PhpRQ;

/**
 * Common functionality for all the queue classes
 */
abstract class Base
{
    /**
     * @var ClientInterface
     */
    protected $redis;

    /**
     * @param ClientInterface $redis
     *
     */
    public function __construct(ClientInterface $redis)
    {
        $this->redis = $redis;
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

}
