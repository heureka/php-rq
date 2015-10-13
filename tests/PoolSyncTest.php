<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class PoolSyncTest extends BaseTestCase
{

    public function testAddItemSync()
    {
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        try {
            $pool->addItem(1);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(['1' => (string)self::TIME_MOCK], $this->redis->zrange('test', 0, 5, 'WITHSCORES'));
        $this->assertKeys(['test']);
    }

    public function testAddItemsSync()
    {
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        try {
            $pool->addItems([1, 3, 5, 3]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(
            array_fill_keys(['1', '3', '5'], (string)self::TIME_MOCK),
            $this->redis->zrange('test', 0, 5, 'WITHSCORES')
        );
        $this->assertKeys(['test']);
    }

    public function testAckItemSync()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10 + 600.1,
            3 => self::TIME_MOCK - 5  + 600.1,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        try {
            $pool->ackItem(1);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 1));
        $this->assertSame(round(self::TIME_MOCK - 5 + 600.1), round($this->redis->zscore('test', 3)));
        $this->assertSame(round(self::TIME_MOCK - 2 + 600.1), round($this->redis->zscore('test', 5)));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testAckItemsSync()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10 + 600.1,
            3 => self::TIME_MOCK - 5  + 600.1,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        try {
            $pool->ackItems([5, 3]);
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertSame(5, $this->redis->zcard('test'));
        $this->assertSame(round(self::TIME_MOCK - 10 + 600.1), round($this->redis->zscore('test', 1)));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 3));
        $this->assertSame(self::TIME_MOCK + 129600, (int)$this->redis->zscore('test', 5));
        $this->assertSame(round(self::TIME_MOCK + 600.1, 1), round($this->redis->zscore('test', 7), 1));
        $this->assertSame(self::TIME_MOCK + 5, (int)$this->redis->zscore('test', 9));
        $this->assertKeys(['test']);
    }

    public function testRemoveItemSync()
    {
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2 + 600.1,
            7 => self::TIME_MOCK     + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

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
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

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
        $this->redis->zadd('test', [
            1 => self::TIME_MOCK - 10,
            3 => self::TIME_MOCK - 5,
            5 => self::TIME_MOCK - 2  + 600.1,
            7 => self::TIME_MOCK      + 600.1,
            9 => self::TIME_MOCK + 5,
        ]);
        $pool = new Pool($this->redis, 'test', [
            Base::OPT_SLAVES_SYNC_ENABLED        => true,
            Base::OPT_SLAVES_SYNC_REQUIRED_COUNT => 5,
        ], $this->getTimeMock());

        try {
            $pool->clearPool();
        } catch (\PhpRQ\Exception\NotEnoughSlavesSynced $e) {}

        $this->assertKeys([]);
    }

}
