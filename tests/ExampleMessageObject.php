<?php

namespace PhpRQ;

/**
 * Class used in tests to simulate more sophisticated messages
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class ExampleMessageObject
{

    public $int;

    public $float;

    public $string;

    public $bool;

    public function __toString()
    {
        return serialize($this);
    }

}
