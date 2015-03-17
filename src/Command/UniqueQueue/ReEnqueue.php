<?php

namespace PhpRQ\Command\UniqueQueue;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class ReEnqueue extends \Predis\Command\ScriptCommand
{

    /**
     * @return int The number of the keys passed to the method as arguments
     */
    protected function getKeysCount()
    {
        return 4;
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
local processing = KEYS[3]
local timeouts = KEYS[4]

local item
local inQueue
while true do
    item = redis.call('lpop', processing);

    if not item then
        break
    end

    inQueue = redis.call('sismember', set, item)
    if inQueue == 0 then
        redis.call('rpush', queue, item)
        redis.call('sadd', set, item)
    else
        redis.call('lrem', queue, -1, item)
        redis.call('rpush', queue, item)
    end
end

redis.call('hdel', timeouts, processing)
LUA;
    }

}
