<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class TimeTest extends BaseTestCase
{

    public function testTime()
    {
        $time = new Time();

        $this->assertTrue(is_int($time->now()));
    }

    public function testMicro()
    {
        $time = new Time();

        $this->assertTrue(is_float($time->micro()));
    }

}
