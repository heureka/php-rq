<?php

namespace PhpRQ;

use Predis\Collection\Iterator\Keyspace;

/**
 * This class is a base class for all the PhpRQ tests. It ensures that each test will receive properly configured
 * instance of Redis client.
 *
 * The Redis client is required to point to an empty database. PhpRQ won't perform any cleaning before or after testing
 * at all, so the client provider is also required to perform that cleaning.
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
abstract class BaseTestCase extends \PHPUnit\Framework\TestCase
{

    /**
     * @var int UNIX timestamp mock
     */
    const TIME_MOCK = 1444222459;

    /**
     * @var float UNIX timestamp + microseconds mock
     */
    const MICRO_TIME_MOCK = 1444222459.1847;

    /**
     * @var ClientInterface
     */
    protected $redis;

    /**
     * @param ClientInterface $redis Redis client instance pointing to an empty database. PhpRQ tests won't perform
     *                               any cleaning before or after testing at all.
     */
    public function __construct(ClientInterface $redis)
    {
        $this->redis = $redis;
    }

    /**
     * @return Time|\Mockery\MockInterface
     */
    protected function getTimeMock()
    {
        $mock = \Mockery::mock('PhpRQ\Time');

        $mock->shouldReceive('now')
            ->zeroOrMoreTimes()
            ->withNoArgs()
            ->andReturn(self::TIME_MOCK);

        $mock->shouldReceive('micro')
            ->zeroOrMoreTimes()
            ->withNoArgs()
            ->andReturn(self::MICRO_TIME_MOCK);

        return $mock;
    }

    /**
     * This method is intended for testing the keys in the current database.
     * You should use this method in each test to be sure that there are only the keys you expect in the database
     *
     * @param string[] $expectedKeys
     */
    protected function assertKeys(array $expectedKeys)
    {
        $keys = iterator_to_array(new Keyspace($this->redis, '*'));

        $unexpected = array_diff($keys, $expectedKeys);

        if (!empty($unexpected)) {
            $this->fail('There are some unexpected keys in the database: ' . implode(', ', $unexpected));
        }

        $missing = array_diff($expectedKeys, $keys);

        if (!empty($missing)) {
            $this->fail('Some of the expected keys are missing in the database: ' . implode(', ', $missing));
        }
    }

}
