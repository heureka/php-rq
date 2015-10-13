<?php

namespace PhpRQ;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Time
{

    /**
     * @return int Current UNIX timestamp
     */
   public function now()
   {
       return time();
   }

    /**
     * @return float Current UNIX timestamp + microseconds part
     */
    public function micro()
    {
        return microtime($getAsFloat = true);
    }

}
