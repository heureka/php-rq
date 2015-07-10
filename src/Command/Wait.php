<?php

namespace PhpRQ\Command;

/**
 * @todo replace with predis command when it is implemented
 *
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Wait extends \Predis\Command\Command
{

    /**
     * {@inheritdoc}
     */
    public function getId()
    {
        return 'WAIT';
    }
}
