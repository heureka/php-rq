<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Client extends \Predis\Client implements ClientInterface
{

    /**
     * @param mixed $parameters Connection parameters for one or more servers.
     * @param mixed $options    Options to configure some behaviours of the client.
     */
    public function __construct($parameters = null, $options = null)
    {
        parent::__construct($parameters, $options);

        /** @var \Predis\Profile\RedisProfile $profile */
        $profile = $this->getProfile();

        $profile->defineCommand('queueGet', 'PhpRQ\Command\Queue\Get');
        $profile->defineCommand('queueAck', 'PhpRQ\Command\Queue\Ack');
        $profile->defineCommand('queueReject', 'PhpRQ\Command\Queue\Reject');
        $profile->defineCommand('queueReEnqueue', 'PhpRQ\Command\Queue\ReEnqueue');

        $profile->defineCommand('uniqueQueueAdd', 'PhpRQ\Command\UniqueQueue\Add');
        $profile->defineCommand('uniqueQueueGet', 'PhpRQ\Command\UniqueQueue\Get');
        $profile->defineCommand('uniqueQueueAck', 'PhpRQ\Command\UniqueQueue\Ack');
        $profile->defineCommand('uniqueQueueReject', 'PhpRQ\Command\UniqueQueue\Reject');
        $profile->defineCommand('uniqueQueueReEnqueue', 'PhpRQ\Command\UniqueQueue\ReEnqueue');

        $profile->defineCommand('poolGet', 'PhpRQ\Command\Pool\Get');
        $profile->defineCommand('poolAck', 'PhpRQ\Command\Pool\Ack');
        $profile->defineCommand('poolRemove', 'PhpRQ\Command\Pool\Remove');

        $profile->defineCommand('wait', 'PhpRQ\Command\Wait');
    }

    /**
     * @inheritdoc
     */
    public function queueGet($queue, $processing, $timeouts, $size, $time)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function queueAck($processing, $timeouts, $item)
    {
        parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function queueReject($queue, $processing, $timeouts, $item)
    {
        parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function queueReEnqueue($queue, $processing, $timeouts)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function uniqueQueueAdd($queue, $set, $item)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function uniqueQueueGet($queue, $set, $processing, $timeouts, $size, $time)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function uniqueQueueAck($processing, $timeouts, $item)
    {
        parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function uniqueQueueReject($queue, $set, $processing, $timeouts, $item)
    {
        parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function uniqueQueueReEnqueue($queue, $set, $processing, $timeouts)
    {
        parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function poolGet($pool, $size, $until, $ackTTL)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function poolAck($pool, $item, $processAgainAt)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function poolRemove($pool, $item)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

    /**
     * @inheritdoc
     */
    public function wait($numberOfSlaves, $timeout)
    {
        return parent::__call(__FUNCTION__, func_get_args());
    }

}
