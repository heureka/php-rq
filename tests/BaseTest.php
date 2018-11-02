<?php

namespace PhpRQ;

use Mockery;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class BaseTest extends \PHPUnit\Framework\TestCase
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

    public function testSafeExecutionSuccesful()
    {
        $queue = new TestQueueImplementation($this->getRedisMock(), 'test');

        $testValue = 'whateva';

        $assertMock = \Mockery::mock();
        $assertMock->shouldReceive('success')->once();
        $assertMock->shouldReceive('fail')->never();

        $this->assertSame(
            $testValue,
            $queue->safeExecution(function($queue) use($testValue) {return $testValue;}),
            function($returnValue) use ($testValue, $assertMock) {
                $this->assertSame($testValue, $returnValue);
                $assertMock->success();
            },
            function() use ($assertMock){$assertMock->fail();}
        );
    }

    public function testSafeExecution2retries()
    {
        $queue = new TestQueueImplementation($this->getRedisMock(), 'test');

        $assertMock = \Mockery::mock();
        $assertMock->shouldReceive('success')->once();
        $assertMock->shouldReceive('fail')->twice();

        $this->assertSame(5, $queue->safeExecution(
            function($queue) {return $queue->raise2timesException();},
            function() use ($assertMock) {$assertMock->success();},
            function() use ($assertMock) {$assertMock->fail();}
        ));
    }

    public function testSafeExecutionAllRetriesFail()
    {
        $queue = new TestQueueImplementation($this->getRedisMock(), 'test');

        $assertMock = \Mockery::mock();
        $assertMock->shouldReceive('success')->never();
        $assertMock->shouldReceive('fail')->once();

        try {
            $queue->safeExecution(
                function($queue) {return $queue->raiseAllwaysException();},
                function() use ($assertMock) {$assertMock->success();},
                function() use ($assertMock) {$assertMock->fail();}
            );
            $this->fail('The ConnectionException should have been raised.');
        } catch (\Predis\Connection\ConnectionException $e) {}
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

    public function raiseAllwaysException()
    {
        $connectionInterface = \Mockery::mock(\Predis\Connection\NodeConnectionInterface::class);
        throw new \Predis\Connection\ConnectionException($connectionInterface, 'test');
    }

    private $errorCounter = 0;

    public function raise2timesException()
    {
        $connectionInterface = \Mockery::mock(\Predis\Connection\NodeConnectionInterface::class);

        $this->errorCounter++;
        if ($this->errorCounter < 2) {
            throw new \Predis\Connection\ConnectionException($connectionInterface, 'test');
        } else {
            return 5;
        }
    }

}
