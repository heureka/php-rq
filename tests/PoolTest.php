<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class PoolTest extends BaseTestCase
{

    public function testGetRedisClient()
    {
        $pool = new Pool($this->redis, 'test');
        $this->assertSame($this->redis, $pool->getRedisClient());
    }

    public function testGetPoolName()
    {
        $poolName = 'testPoolName-cz:xy';
        $pool = new Pool($this->redis, $poolName);
        $this->assertSame($poolName, $pool->getName());
    }

    public function testGetCount()
    {
        $this->redis->zadd('test', [1 => 123, 2 => 456, 3 => 789]);
        $pool = new Pool($this->redis, 'test');

        $this->assertSame(3, $pool->getCount());
        $this->assertKeys(['test']);
    }

    /**
     * @param int $processTimeOffset
     * @param int $expectedCount
     *
     * @dataProvider providerGetCountToProcess
     */
    public function testGetCountToProcess($processTimeOffset, $expectedCount)
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            2 => self::TIME_MOCK - 5,
            3 => self::TIME_MOCK - 3,
            4 => self::TIME_MOCK,
            5 => self::TIME_MOCK + 5,
        ]);
        $options = [Pool::OPT_PROCESS_TIME_OFFSET => $processTimeOffset];
        $pool = new Pool($this->redis, 'test', $options, $this->getTimeMock());

        $this->assertSame($expectedCount, $pool->getCountToProcess());
        $this->assertKeys(['test']);
    }

    /**
     * @return array
     */
    public function providerGetCountToProcess()
    {
        return [
            [0, 4],
            [4, 2],
            [3, 3],
        ];
    }

    public function testIsInPool()
    {
        $this->redis->zadd('test', [1 => 123, 2 => 456, 3 => 789, 4 => 987]);
        $pool = new Pool($this->redis, 'test');

        $this->assertTrue($pool->isInPool(1));
        $this->assertTrue($pool->isInPool(4));
        $this->assertFalse($pool->isInPool(6));
        $this->assertKeys(['test']);
    }

    public function testIsInPoolArray()
    {
        $this->redis->zadd('test', [1 => 123, 2 => 456, 3 => 789, 4 => 987]);
        $pool = new Pool($this->redis, 'test');

        $this->assertSame([1 => true, 4 => true], $pool->isInPool([1, 4]));
        $this->assertSame([6 => false, 3 => true, 1 => true, 9 => false], $pool->isInPool([6, 3, 1, 9]));
        $this->assertKeys(['test']);
    }

    public function testAddItem()
    {
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());
        $pool->addItem(1);
        $pool->addItem(3);
        $pool->addItem(5);
        $pool->addItem(3);

        $this->assertSame(
            array_fill_keys(['1', '3', '5'], (string)self::TIME_MOCK),
            $this->redis->zrange('test', 0, 5, 'WITHSCORES')
        );
        $this->assertKeys(['test']);
    }

    /**
     * @dataProvider providerAddItemsEmpty
     */
    public function testAddItemEmpty($emptyItem = null)
    {
        $pool = new Pool($this->redis, 'test');
        $pool->addItem(1);

        try {
            $pool->addItem($emptyItem);
            $this->fail('Expected \PhpRQ\Exception\InvalidArgument to be thrown');
        } catch (Exception\InvalidArgument $e) {}

        $this->assertSame(['1'], $this->redis->zrange('test', 0, 5));
        $this->assertKeys(['test']);
    }

    public function testAddItems()
    {
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());
        $pool->addItems([1, 3, 5, 3]);
        $pool->addItems([3, 6]);

        $this->assertSame(
            array_fill_keys(['1', '3', '5', '6'], (string)self::TIME_MOCK),
            $this->redis->zrange('test', 0, 5, 'WITHSCORES')
        );
        $this->assertKeys(['test']);
    }

    /**
     * @dataProvider providerAddItemsEmpty
     */
    public function testAddItemsEmpty($emptyItem = null)
    {
        $pool = new Pool($this->redis, 'test');
        try {
            $pool->addItems([1, $emptyItem, 5]);
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
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 5,
            3 => self::TIME_MOCK - 2,
            5 => self::TIME_MOCK,
            7 => self::TIME_MOCK + 2,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $items = $pool->getItems(2);
        $this->assertSame(['1', '3'], $items);

        $items = $pool->getItems(2);
        $this->assertSame(['5'], $items);

        $items = $pool->getItems(2);
        $this->assertSame([], $items);

        $this->assertKeys(['test']);
    }

    public function testGetItemsWithNonZeroTimeOffset()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 5,
            3 => self::TIME_MOCK - 2,
            5 => self::TIME_MOCK,
            7 => self::TIME_MOCK + 2,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [Pool::OPT_PROCESS_TIME_OFFSET => 2], $this->getTimeMock());

        $items = $pool->getItems(2);
        $this->assertSame(['1', '3'], $items);

        $items = $pool->getItems(2);
        $this->assertSame([], $items);

        $this->assertKeys(['test']);
    }

    /**
     * @dataProvider providerGetItemsInvalid
     */
    public function testGetItemsInvalid($count = null)
    {
        $pool = new Pool($this->redis, 'test');
        try {
            $pool->getItems($count);
            $this->fail('Expected \PhpRQ\Exception\InvalidArgument to be thrown');
        } catch (Exception\InvalidArgument $e) {}
    }

    public function providerGetItemsInvalid()
    {
        return [0, -5, '', null, false];
    }

    public function testGetAllItems()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2,
            7 => self::TIME_MOCK,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $items = $pool->getAllItems();
        $this->assertSame(['1', '3', '5', '7'], $items);

        $items = $pool->getAllItems();
        $this->assertSame([], $items);

        $this->assertKeys(['test']);
    }

    public function testGetAllItemsWithNonZeroTimeOffset()
    {
        $this->redis->zadd('test', [
            1  => self::TIME_MOCK - 10,
            3  => self::TIME_MOCK - 5,
            5  => self::TIME_MOCK - 2,
            7  => self::TIME_MOCK - 1,
            9  => self::TIME_MOCK,
            11 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [Pool::OPT_PROCESS_TIME_OFFSET => 2], $this->getTimeMock());

        $items = $pool->getAllItems();
        $this->assertSame(['1', '3', '5'], $items);

        $items = $pool->getAllItems();
        $this->assertSame([], $items);

        $this->assertKeys(['test']);
    }

    public function testAckItem()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10 + 600.1,
            3 => self::TIME_MOCK - 5  + 600.1,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $pool->ackItem(1);
        $pool->ackItem(5);
        $pool->ackItem(3);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testAckItems()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10 + 600.1,
            3 => self::TIME_MOCK - 5  + 600.1,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $pool->ackItems([1]);
        $pool->ackItems([5, 3]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testRemoveItem()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2 + 600.1,
            7 => self::TIME_MOCK     + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $pool->removeItem(1);
        $pool->removeItem(7);
        $pool->removeItem(5);

        $this->assertSame(3, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 5);
        $this->assertTrue(in_array('1', $items, true));
        $this->assertTrue(in_array('3', $items, true));
        $this->assertTrue(in_array('9', $items, true));
        $this->assertKeys(['test']);
    }

    public function testRemoveItems()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());

        $pool->removeItems([1]);
        $pool->removeItems([7, 5]);

        $this->assertSame(3, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 5);
        $this->assertTrue(in_array('1', $items, true));
        $this->assertTrue(in_array('3', $items, true));
        $this->assertTrue(in_array('9', $items, true));
        $this->assertKeys(['test']);
    }

    public function testClearPool()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());
        $pool->clearPool();

        $this->assertKeys([]);
    }

    public function testRealUseCaseExample1()
    {
        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());
        $pool->addItems([1, 2, 3, 4, 5, 6, 7]);

        $this->assertSame(7, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 10);
        $this->assertTrue(in_array('1', $items, true));
        $this->assertTrue(in_array('2', $items, true));
        $this->assertTrue(in_array('3', $items, true));
        $this->assertTrue(in_array('4', $items, true));
        $this->assertTrue(in_array('5', $items, true));
        $this->assertTrue(in_array('6', $items, true));
        $this->assertTrue(in_array('7', $items, true));

        $this->redis->zadd('test', [7 => self::TIME_MOCK + 5]);

        $items = $pool->getItems(3);
        $this->assertSame(['1', '2', '3'], $items);

        $pool->ackItem(1);
        $pool->ackItem(3);

        $items = $pool->getItems(3);
        $this->assertSame(['4', '5', '6'], $items);

        $pool->ackItems([2, 4, 5]);

        $this->assertSame(7, $this->redis->zcard('test'));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 2));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 4));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', 6), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', 7));

        $pool->removeItem(7);

        $this->assertSame(7, $this->redis->zcard('test'));

        $pool->removeItem(6);

        $this->assertSame(6, $this->redis->zcard('test'));
        $this->assertKeys(['test']);
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

        $pool = new Pool($this->redis, 'test', [], $this->getTimeMock());
        $pool->addItems([$message1, $message2, $message3, $message4, $message5]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 10);
        $this->assertTrue(in_array($message1Serialized, $items, true));
        $this->assertTrue(in_array($message2Serialized, $items, true));
        $this->assertTrue(in_array($message3Serialized, $items, true));
        $this->assertTrue(in_array($message4Serialized, $items, true));
        $this->assertTrue(in_array($message5Serialized, $items, true));

        $this->redis->zadd('test', [$message5Serialized => self::TIME_MOCK + 5]);

        $items = $pool->getItems(3);
        $this->assertSame([$message1Serialized, $message2Serialized, $message3Serialized], $items);

        $pool->ackItem($message1);
        $pool->ackItem($message3);

        $items = $pool->getItems(3);
        $this->assertSame([$message4Serialized], $items);

        $pool->ackItems([$message2Serialized]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', $message1Serialized));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', $message2Serialized));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', $message3Serialized));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', $message4Serialized), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', $message5Serialized));

        $pool->removeItem($message5);

        $this->assertSame(5, $this->redis->zcard('test'));

        $pool->removeItem($message4);

        $this->assertSame(4, $this->redis->zcard('test'));
        $this->assertKeys(['test']);
    }

}
