<?php

namespace PhpRQ\Command\UniqueQueue;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Reject extends \Predis\Command\ScriptCommand
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
local item = ARGV[1]

local removed = redis.call('lrem', processing, -1, item)

if removed == 1 then
    local inQueue = redis.call('sismember', set, item)
    if inQueue == 0 then
        redis.call('rpush', queue, item)
        redis.call('sadd', set, item)
    end
end

local count = redis.call('llen', processing)
if count == 0 then
    redis.call('hdel', timeouts, processing)
end
LUA;
    }

}
