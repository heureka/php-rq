<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class PoolSyncTest extends BaseTestCase
{

    public function testAddItemSync()
    {
        $time = time();
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->addItem(1);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['1' => (string)$time], $this->redis->zrange('test', 0, 5, 'WITHSCORES'));
        $this->assertKeys(['test']);
    }

    public function testAddItemsSync()
    {
        $time = time();
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->addItems([1, 3, 5, 3]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(
            array_fill_keys(['1', '3', '5'], (string)$time),
            $this->redis->zrange('test', 0, 5, 'WITHSCORES')
        );
        $this->assertKeys(['test']);
    }

    public function testAckItemSync()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10 + 600.1,
            3 => $time - 5  + 600.1,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->ackItem(1);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame(round($time - 5 + 600.1), round($this->redis->zscore('test', 3)));
        $this->assertSame(round($time - 2 + 600.1), round($this->redis->zscore('test', 5)));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testAckItemsSync()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10 + 600.1,
            3 => $time - 5  + 600.1,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->ackItems([5, 3]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(round($time - 10 + 600.1), round($this->redis->zscore('test', 1)));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame($time + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round($time + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame($time + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testRemoveItemSync()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2 + 600.1,
            7 => $time     + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->removeItem(5);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(4, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 5);
        $this->assertTrue(in_array('1', $items, true));
        $this->assertTrue(in_array('3', $items, true));
        $this->assertTrue(in_array('7', $items, true));
        $this->assertTrue(in_array('9', $items, true));
        $this->assertKeys(['test']);
    }

    public function testRemoveItemsSync()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->removeItems([7, 5, 3]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(3, $this->redis->zcard('test'));
        $items = $this->redis->zrange('test', 0, 5);
        $this->assertTrue(in_array('1', $items, true));
        $this->assertTrue(in_array('3', $items, true));
        $this->assertTrue(in_array('9', $items, true));
        $this->assertKeys(['test']);
    }

    public function testClearPoolSync()
    {
        $time = time();
        $this->redis->zadd('test', [
            1 => $time - 10,
            3 => $time - 5,
            5 => $time - 2  + 600.1,
            7 => $time      + 600.1,
            9 => $time + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ]);

        try {
            $pool->clearPool();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertKeys([]);
    }

}
