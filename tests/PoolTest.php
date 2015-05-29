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
        $this->assertSame($poolName, $pool->getPoolName());
    }

    public function testGetCount()
    {
        $this->redis->zadd('test', [1 => 123, 2 => 456, 3 => 789]);
        $pool = new Pool($this->redis, 'test');

        $this->assertSame(3, $pool->getCount());
        $this->assertKeys(['test']);
    }

    public function testGetCountToProcess()
    {
        $time = time();
        $this->redis->zadd('test', [1 => $time - 5, 2 => $time - 3, 3 => $time + 5]);
        $pool = new Pool($this->redis, 'test');

        $this->assertSame(2, $pool->getCountToProcess());
        $this->assertKeys(['test']);
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
        $time = time();
        $pool = new Pool($this->redis, 'test');
        $pool->addItem(1);
        $pool->addItem(3);
        $pool->addItem(5);
        $pool->addItem(3);

        $this->assertSame(
            array_fill_keys(['1', '3', '5'], (string)$time),
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
        $time = time();
        $pool = new Pool($this->redis, 'test');
        $pool->addItems([1, 3, 5, 3]);
        $pool->addItems([3, 6]);

        $this->assertSame(
            array_fill_keys(['1', '3', '5', '6'], (string)$time),
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
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 5,
            3 => $time - 2,
            5 => $time,
            7 => $time + 2,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

        $items = $pool->getItems(2);
        $this->assertSame(['1', '3'], $items);

        $items = $pool->getItems(2);
        $this->assertSame(['5'], $items);

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
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2,
            7 => $time,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

        $items = $pool->getAllItems();
        $this->assertSame(['1', '3', '5', '7'], $items);

        $items = $pool->getAllItems();
        $this->assertSame([], $items);

        $this->assertKeys(['test']);
    }

    public function testAckItem()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10 + 600.1,
            3 => $time - 5  + 600.1,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

        $pool->ackItem(1);
        $pool->ackItem(5);
        $pool->ackItem(3);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testAckItems()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10 + 600.1,
            3 => $time - 5  + 600.1,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

        $pool->ackItems([1]);
        $pool->ackItems([5, 3]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testRemoveItem()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2 + 600.1,
            7 => $time     + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

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
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');

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
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test');
        $pool->clearPool();

        $this->assertKeys([]);
    }

    public function testRealUseCaseExample1()
    {
        $time = time();
        $pool = new Pool($this->redis, 'test');
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

        $this->redis->zadd('test', [7 => $time + 5]);

        $items = $pool->getItems(3);
        $this->assertSame(['1', '2', '3'], $items);

        $pool->ackItem(1);
        $pool->ackItem(3);

        $items = $pool->getItems(3);
        $this->assertSame(['4', '5', '6'], $items);

        $pool->ackItems([2, 4, 5]);

        $this->assertSame(7, $this->redis->zcard('test'));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 2));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 4));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', 6), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', 7));

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

        $time = time();
        $pool = new Pool($this->redis, 'test');
        $pool->addItems([$message1, $message2, $message3, $message4, $message5]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 10);
        $this->assertTrue(in_array($message1Serialized, $items, true));
        $this->assertTrue(in_array($message2Serialized, $items, true));
        $this->assertTrue(in_array($message3Serialized, $items, true));
        $this->assertTrue(in_array($message4Serialized, $items, true));
        $this->assertTrue(in_array($message5Serialized, $items, true));

        $this->redis->zadd('test', [$message5Serialized => $time + 5]);

        $items = $pool->getItems(3);
        $this->assertSame([$message1Serialized, $message2Serialized, $message3Serialized], $items);

        $pool->ackItem($message1);
        $pool->ackItem($message3);

        $items = $pool->getItems(3);
        $this->assertSame([$message4Serialized], $items);

        $pool->ackItems([$message2Serialized]);

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', $message1Serialized));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', $message2Serialized));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', $message3Serialized));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', $message4Serialized), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', $message5Serialized));

        $pool->removeItem($message5);

        $this->assertSame(5, $this->redis->zcard('test'));

        $pool->removeItem($message4);

        $this->assertSame(4, $this->redis->zcard('test'));
        $this->assertKeys(['test']);
    }

}
