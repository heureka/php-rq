<?php

namespace PhpRQ\Command\UniqueQueue;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Get extends \Predis\Command\ScriptCommand
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
local size = ARGV[1]
local time = ARGV[2]

redis.call('hset', timeouts, processing, time)

local item
local items = {}
for i = 1, size, 1 do
    item = redis.call('rpoplpush', queue, processing)

    if not item then
        break
    end

    redis.call('srem', set, item)
    table.insert(items, item)
end

return items
LUA;
    }

}
