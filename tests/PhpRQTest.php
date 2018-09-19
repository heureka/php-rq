<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class PhpRQTest extends \PHPUnit\Framework\TestCase
{

    /**
     * @var Client
     */
    public static $redis;

    public function testPhpRQ()
    {
        if (!self::$redis instanceof Client) {
            throw new \Exception(
                'The PhpRQ test suit can be run only with php-rq-run-tests binary or by implementing
                the ClientProvider and running the TestRunner in your test suite.'
            );
        }

        $redisProvider = new ClientProvider;
        $redisProvider->registerProvider(function() {
            self::$redis->flushdb();
            self::$redis->script('flush');

            return self::$redis;
        });

        $test = new TestRunner($redisProvider);
        $test->run();
    }

}
