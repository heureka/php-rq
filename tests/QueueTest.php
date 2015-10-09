<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class QueueTest extends BaseTestCase
{

    public function testGetRedisClient()
    {
        $queue = new Queue($this->redis, 'test');
        $this->assertSame($this->redis, $queue->getRedisClient());
    }

    public function testGetQueueName()
    {
        $queueName = 'testQueueName-cz:xy';
        $queue = new Queue($this->redis, $queueName);
        $this->assertSame($queueName, $queue->getName());
    }

    public function testGetCount()
    {
        $this->redis->lpush('test', [1, 5, 3]);

        $queue = new Queue($this->redis, 'test');

        $this->assertSame(3, $queue->getCount());
        $this->assertKeys(['test']);
    }

    public function testAddItem()
    {
        $queue = new Queue($this->redis, 'test');
        $queue->addItem(1);
        $queue->addItem(3);
        $queue->addItem(5);
        $queue->addItem(3);

        $this->assertSame(['3', '5', '3', '1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    /**
     * @dataProvider providerAddItemsEmpty
     */
    public function testAddItemEmpty($invalidItem = null)
    {
        $queue = new Queue($this->redis, 'test');
        $queue->addItem(1);

        try {
            $queue->addItem($invalidItem);
            $this->fail('Expected \PhpRQ\Exception\InvalidArgument to be thrown');
        } catch (Exception\InvalidArgument $e) {}

        $this->assertSame(['1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    public function testAddItems()
    {
        $queue = new Queue($this->redis, 'test');
        $queue->addItems([1, 3, 5]);
        $queue->addItems([3, 6]);

        $this->assertSame(['6', '3', '5', '3', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    /**
     * @dataProvider providerAddItemsEmpty
     */
    public function testAddItemsEmpty($emptyItem = null)
    {
        $queue = new Queue($this->redis, 'test');

        try {
            $queue->addItems([1, $emptyItem, 5]);
            $this->fail('Expected \PhpRQ\Exception\InvalidArgument to be thrown');
        } catch (Exception\InvalidArgument $e) {}

        $this->assertKeys([]);
    }

    public function providerAddItemsEmpty()
    {
        return ['', null, 0, false];
    }

    public function testGetItems()
    {
        $this->redis->lpush('test', [1, 5, 5, 3]);

        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $items = $queue->getItems(3);
        $this->assertSame(['1', '5', '5'], $items);

        $items = $queue->getItems(3);
        $this->assertSame(['3'], $items);

        $items = $queue->getItems(3);
        $this->assertSame([], $items);
        $this->assertKeys([
            sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK),
            'test-timeouts',
        ]);
    }

    /**
     * @dataProvider providerGetItemsInvalid
     */
    public function testGetItemsInvalid($count = null)
    {
        $queue = new Queue($this->redis, 'test');
        try {
            $queue->getItems($count);
            $this->fail('Expected \PhpRQ\Exception\InvalidArgument to be thrown');
        } catch (Exception\InvalidArgument $e) {}
    }

    public function providerGetItemsInvalid()
    {
        return [0, -5, '', null, false];
    }

    public function testGetAllItems()
    {
        $this->redis->lpush('test', [1, 5, 5, 3]);

        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $items = $queue->getAllItems();
        $this->assertSame(['1', '5', '5', '3'], $items);

        $items = $queue->getAllItems();
        $this->assertSame([], $items);
        $this->assertKeys([
            sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK),
            'test-timeouts',
        ]);
    }

    public function testAckItem()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->ackItem(1);
        $queue->ackItem(5);
        $queue->ackItem(1);

        $this->assertSame(['3', '5'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            $processingQueue,
            'test-timeouts'
        ]);

        $queue->ackItem(3);
        $queue->ackItem(5);

        $this->assertKeys([]);
    }

    public function testAckItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->ackItems([1, 5]);
        $queue->ackItems([1]);

        $this->assertSame(['7', '6', '3', '5'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            $processingQueue,
            'test-timeouts'
        ]);

        $queue->ackItems([5, 3, 6]);
        $queue->ackItems([7]);

        $this->assertKeys([]);
    }

    public function testRejectItem()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->rejectItem(1);
        $queue->rejectItem(5);
        $queue->rejectItem(1);

        // order of the items is lost when using Queue:rejectItem, be aware of that
        $this->assertSame(['1', '5'], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['3', '5'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts'
        ]);

        $queue->rejectItem(3);
        $queue->rejectItem(5);

        // order of the items is lost when using Queue:rejectItem, be aware of that
        $this->assertSame(['1', '5', '3', '5'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);

        $this->redis->lpush($processingQueue, 1);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->rejectItem(1);

        // order of the items is lost when using Queue:rejectItem, be aware of that
        $this->assertSame(['1', '5', '3', '5', '1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    public function testRejectItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->rejectItems([1, 5]);
        $queue->rejectItems([5]);
        $queue->rejectItems([9]);

        // order of the items is preserved with Queue:rejectItems only if there is a reject on all the remaining
        // items at once. Consecutive calls of this method causes the lost of the items order.
        $this->assertSame(['5', '1', '5'], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['7', '6', '3'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertSame([$processingQueue => (string)self::MICRO_TIME_MOCK], $this->redis->hgetall('test-timeouts'));
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts'
        ]);

        $queue->rejectItems([3, 6, 7]);

        // order of the items is preserved with Queue:rejectItems only if there is a reject on all the remaining
        // items at once. Consecutive calls of this method causes the lost of the items order.
        $this->assertSame(['5', '1', '5', '7', '6', '3'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);

        $this->redis->lpush($processingQueue, 1);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->rejectItems([1]);

        // order of the items is preserved with Queue:rejectItems only if there is a reject on all the remaining
        // items at once. Consecutive calls of this method causes the lost of the items order.
        $this->assertSame(['5', '1', '5', '7', '6', '3', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testRejectBatch()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);
        $this->redis->lpush($processingQueue, [1, 5, 5, 3, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue, self::MICRO_TIME_MOCK);

        $queue->ackItem(5);
        $queue->rejectBatch();

        $this->assertSame(['7', '6', '3', '5', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testReEnqueueTimedOutItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        $queue->reEnqueueTimedOutItems(7);

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

        $queue->reEnqueueTimedOutItems(0);

        $this->assertSame(['6', '4', '1', '3', '5', '1', '8', '7', '4'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testReEnqueueAllItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        $queue->reEnqueueAllItems();

        $this->assertSame(['8', '7', '4', '6', '4', '1', '3', '5', '1'], $this->redis->lrange('test', 0, 10));
        $this->assertKeys(['test']);
    }

    public function testDropTimedOutItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        $queue->dropTimedOutItems(7);

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

        $queue->dropTimedOutItems(0);

        $this->assertSame([], $this->redis->lrange('test', 0, 10));
        $this->assertKeys([]);
    }

    public function testDropAllItems()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 5, 3]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [1, 4, 6]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $processingQueue3 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 5);
        $this->redis->lpush($processingQueue3, [4, 7, 8]);
        $this->redis->hset('test-timeouts', $processingQueue3, self::MICRO_TIME_MOCK - 5);

        $queue->dropAllItems();

        $this->assertSame([], $this->redis->lrange('test', 0, 5));
        $this->assertKeys([]);
    }

    public function testClearQueue()
    {
        $queue = new Queue($this->redis, 'test', [UniqueQueue::OPT_DEL_MAX_CHUNK_SIZE => 2], $this->getTimeMock());

        $this->redis->lpush('test', [1, 5, 3]);

        $processingQueue1 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 15);
        $this->redis->lpush($processingQueue1, [1, 8, 4]);
        $this->redis->hset('test-timeouts', $processingQueue1, self::MICRO_TIME_MOCK - 15);

        $processingQueue2 = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK - 10);
        $this->redis->lpush($processingQueue2, [2, 6, 7]);
        $this->redis->hset('test-timeouts', $processingQueue2, self::MICRO_TIME_MOCK - 10);

        $queue->clearQueue();

        $this->assertKeys([]);
    }

    public function testRealUseCaseExample1()
    {
        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());
        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);

        $queue->addItems([1, 2, 3, 1, 5, 6]);
        $items = $queue->getItems(5);

        $this->assertSame(['1', '2', '3', '1', '5'], $items);
        $this->assertSame(['6'], $this->redis->lrange('test', 0, 5));
        $this->assertSame(['5', '1', '3', '2', '1'], $this->redis->lrange($processingQueue, 0, 10));
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts',
        ]);

        $queue->ackItem(1);
        $queue->ackItems([2, 3]);
        $queue->rejectBatch();

        $this->assertSame(['6', '5', '1'], $this->redis->lrange('test', 0, 5));
        $this->assertKeys(['test']);

        $items = $queue->getItems(5);

        $this->assertSame(['1', '5', '6'], $items);
        $this->assertSame(['6', '5', '1'], $this->redis->lrange($processingQueue, 0, 5));
        $this->assertKeys([
            $processingQueue,
            'test-timeouts',
        ]);

        $queue->ackItems([1, 5, 6]);

        $this->assertKeys([]);
    }

    public function testRealUseCaseExample2()
    {
        $message1 = new ExampleMessageObject();
        $message1->int = 1;
        $message1->float = 1.1;
        $message1->string = 'something1';
        $message1->bool = true;
        $message1Serialized = serialize($message1);

        $message2 = new ExampleMessageObject();
        $message2->int = 3;
        $message2->float = 3.3;
        $message2->string = 'something2';
        $message2->bool = false;
        $message2Serialized = serialize($message2);

        $message3 = new ExampleMessageObject();
        $message3->int = 4;
        $message3->float = 4.4;
        $message3->string = 'something3';
        $message3->bool = true;
        $message3Serialized = serialize($message3);

        $message4 = new ExampleMessageObject();
        $message4->int = 5;
        $message4->float = 5.5;
        $message4->string = 'something4';
        $message4->bool = false;
        $message4Serialized = serialize($message4);

        $message5 = new ExampleMessageObject();
        $message5->int = 6;
        $message5->float = 6.6;
        $message5->string = 'something5';
        $message5->bool = true;
        $message5Serialized = serialize($message5);

        $queue = new Queue($this->redis, 'test', [], $this->getTimeMock());
        $processingQueue = sprintf('test-processing-%s[%d][%d]', gethostname(), getmypid(), self::TIME_MOCK);

        $queue->addItems([$message1, $message2, $message3, $message1, $message4, $message5]);
        $items = $queue->getItems(4);

        $this->assertSame([$message1Serialized, $message2Serialized, $message3Serialized, $message1Serialized], $items);
        $this->assertSame([$message5Serialized, $message4Serialized], $this->redis->lrange('test', 0, 5));
        $this->assertSame(
            [$message1Serialized, $message3Serialized, $message2Serialized, $message1Serialized],
            $this->redis->lrange($processingQueue, 0, 10)
        );
        $this->assertKeys([
            'test',
            $processingQueue,
            'test-timeouts',
        ]);

        $queue->ackItem($message1);
        $queue->ackItems([$message2, $message3]);
        $queue->rejectBatch();

        $this->assertSame(
            [$message5Serialized, $message4Serialized, $message1Serialized],
            $this->redis->lrange('test', 0, 5)
        );
        $this->assertKeys([
            'test',
        ]);

        $items = $queue->getItems(5);

        $this->assertSame([$message1Serialized, $message4Serialized, $message5Serialized], $items);
        $this->assertSame(
            [$message5Serialized, $message4Serialized, $message1Serialized],
            $this->redis->lrange($processingQueue, 0, 5)
        );
        $this->assertKeys([
            $processingQueue,
            'test-timeouts',
        ]);

        $queue->ackItems([$message1Serialized, $message4Serialized, $message5Serialized]);

        $this->assertKeys([]);
    }

}
