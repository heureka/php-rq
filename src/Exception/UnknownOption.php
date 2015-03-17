<?php

namespace PhpRQ\Exception;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class UnknownOption extends \Exception
{

    /**
     * @param string $key
     */
    public function __construct($key)
    {
        parent::__construct(sprintf('Unknown option: "%s"', $key));
    }

}
