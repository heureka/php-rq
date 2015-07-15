<?php

namespace PhpRQ;

use Mockery;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class BaseTest extends \PHPUnit_Framework_TestCase
{

    public function testDefaultOptionsWereSet()
    {
        $queue = new TestQueueImplementation($redis = $this->getRedisMock(), $name = 'test', [
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 10
        ]);

        $options = $queue->testGetOptions();
        $this->assertCount(4, $options);
        $this->assertSame(5, $options['test-option']);
        $this->assertSame(false, $options[Base::OPT_SLAVES_SYNC_ENABLED]);
        $this->assertSame(10, $options[Base::OPT_SLAVES_SYNC_REQUIRED_COUNT]);
        $this->assertSame(100, $options[Base::OPT_SLAVES_SYNC_TIMEOUT]);
    }

    public function testGetRedisClient()
    {
        $queue = new TestQueueImplementation($redis = $this->getRedisMock(), $name = 'test');

        $this->assertSame($redis, $queue->getRedisClient());
    }

    public function testGetName()
    {
        $queue = new TestQueueImplementation($this->getRedisMock(), $name = 'test');

        $this->assertSame($name, $queue->getName());
    }

    public function testWaitForSlaveSyncDisabled()
    {
        $redis = $this->getRedisMock();
        $redis->shouldReceive('wait')->never();
        $queue = new TestQueueImplementation($redis, $name = 'test', [Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5]);

        $queue->testWaitForSlaveSync();
        Mockery::close();
    }

    public function testWaitForSlaveSyncExact()
    {
        $redis = $this->getRedisMock();
        $redis->shouldReceive('wait')->once()->with($required = 3, 100)->andReturn($synced = 5);
        $queue = new TestQueueImplementation($redis, $name = 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => $required,
        ]);

        $queue->testWaitForSlaveSync();
        Mockery::close();
    }

    public function testWaitForSlaveSyncEnough()
    {
        $redis = $this->getRedisMock();
        $redis->shouldReceive('wait')->once()->with($required = 3, 100)->andReturn($synced = 5);
        $queue = new TestQueueImplementation($redis, $name = 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => $required,
        ]);

        $queue->testWaitForSlaveSync();
        Mockery::close();
    }

    public function testWaitForSlaveSyncNotEnough()
    {
        $redis = $this->getRedisMock();
        $redis->shouldReceive('wait')->once()->with($required = 3, 100)->andReturn($synced = 2);
        $queue = new TestQueueImplementation($redis, $name = 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => $required,
        ]);

        try {
            $queue->testWaitForSlaveSync();
        } catch (Exception\NotEnoughSlavesSynced $e) {
            if ($e->getMessage() !== sprintf('Required: %d, synced: %d', $required, $synced)) {
                throw $e;
            }
        }

        Mockery::close();
    }

    /**
     * @return Mockery\Mock|ClientInterface
     */
    private function getRedisMock()
    {
        return Mockery::mock('PhpRQ\ClientInterface');
    }

}

class TestQueueImplementation extends Base
{

    protected function setDefaultOptions()
    {
        $this->options = [
            'test-option' => 5,
        ];
    }

    /**
     * @return array
     */
    public function testGetOptions()
    {
        return $this->options;
    }

    public function testWaitForSlaveSync()
    {
        $this->waitForSlaveSync();
    }

}
