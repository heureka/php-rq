<?php

namespace PhpRQ\Command\UniqueQueue;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Add extends \Predis\Command\ScriptCommand
{

    /**
     * @return int The number of the keys passed to the method as arguments
     */
    protected function getKeysCount()
    {
        return 2;
    }

    /**
     * Gets the body of a Lua script.
     *
     * @return string
     */
    public function getScript()
    {
        return <<<'LUA'
local queue = KEYS[1]
local set = KEYS[2]
local item = ARGV[1]

local inQueue = redis.call('sismember', set, item)
if inQueue == 0 then
    redis.call('lpush', queue, item)
    redis.call('sadd', set, item)
end
LUA;
    }
}
