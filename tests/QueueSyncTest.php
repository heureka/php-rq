<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class QueueSyncTest extends BaseTestCase
{

    public function testAddItemSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);
        try {
            $queue->addItem(1);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    public function testAddItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);
        try {
            $queue->addItems([1, 3, 5]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['5', '3', '1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    public function testAckItemSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        try {
            $queue->ackItem(5);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['3', '5', '1'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            $processingQueue,
            'test-timeouts'
        ]);
    }

    public function testAckItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        try {
            $queue->ackItems([1, 5]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['3', '5'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            $processingQueue,
            'test-timeouts'
        ]);
    }

    public function testRejectItemSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        try {
            $queue->rejectItem(5);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        // order of the items is lost when using Queue:rejectItem, be aware of that
        $this->assertSame(['5'], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['3', '5', '1'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts'
        ]);
    }

    public function testRejectItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        try {
            $queue->rejectItems([1, 5]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        // order of the items is preserved with Queue:rejectItems only if there is a reject on all the remaining
        // items at once. Consecutive calls of this method causes the lost of the items order.
        $this->assertSame(['5', '1'], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['7', '6', '3', '5'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts'
        ]);
    }

    public function testRejectBatchSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        try {
            $queue->ackItem(5);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}
        try {
            $queue->rejectBatch();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['7', '6', '3', '5', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testReEnqueueTimedOutItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        try {
            $queue->reEnqueueTimedOutItems(7);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['6', '4', '1', '3', '5', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertSame(['8', '7', '4'], $this->redis->lrange($processingQueue3, 0, 5));
        $this->assertSame(
            [$processingQueue3 => (string)(self::MICRO_TIME_MOCK - 5)],
            $this->redis->hgetall('test-timeouts')
        );
        $this->assertKeys([
            'test',
            'test-timeouts',
            $processingQueue3,
        ]);
    }

    public function testReEnqueueAllItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        try {
            $queue->reEnqueueAllItems();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['8', '7', '4', '6', '4', '1', '3', '5', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testDropTimedOutItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);


        try {
            $queue->dropTimedOutItems(7);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame([], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['8', '7', '4'], $this->redis->lrange($processingQueue3, 0, 5));
        $this->assertSame(
            [$processingQueue3 => (string)(self::MICRO_TIME_MOCK - 5)],
            $this->redis->hgetall('test-timeouts')
        );
        $this->assertKeys([
            'test-timeouts',
            $processingQueue3,
        ]);
    }

    public function testDropAllItemsSync()
    {
        $queue = new Queue($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        try {
            $queue->dropAllItems();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame([], $this->redis->lrange('test', 0, 5));
        $this->assertKeys([]);
    }

    public function testClearQueueSync()
    {
        $queue = new Queue($this->redis, 'test', [
            UniqueQueue::OPT_DEL_MAX_CHUNK_SIZE  => 2,
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        $this->redis->lpush('test', [1, 5, 3]);

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 8, 4]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [2, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        try {
            $queue->clearQueue();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertKeys([]);
    }

}
