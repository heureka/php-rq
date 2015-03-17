<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class ClientProvider
{

    /**
     * @var callable
     */
    private $provider;

    /**
     * @param callable $provider
     */
    public function registerProvider(callable $provider)
    {
        $this->provider = $provider;
    }

    /**
     * @return ClientInterface
     * @throws ProviderNotRegisteredException
     * @throws InvalidRedisClientException
     */
    public function getRedisClient()
    {
        if ($this->provider === null) {
            throw new ProviderNotRegisteredException('You must first register the provider before calling this method');
        }

        $redis = call_user_func($this->provider);
        if (!($redis instanceof ClientInterface)) {
            if (is_object($redis)) {
                $name = get_class($redis);
            } else {
                $name = gettype($redis);
            }

            throw new InvalidRedisClientException('Expected PhpRQ\ClientInterface, got ' . $name);
        }

        return $redis;
    }

}

class ProviderNotRegisteredException extends \Exception {}
class InvalidRedisClientException extends \Exception {}
