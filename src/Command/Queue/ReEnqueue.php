<?php

namespace PhpRQ\Command\Queue;

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
        return 3;
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
local processing = KEYS[2]
local timeouts = KEYS[3]

local item
while true do
    item = redis.call('lpop', processing);

    if not item then
        break
    end

    redis.call('rpush', queue, item)
end

redis.call('hdel', timeouts, processing)
LUA;
    }
}
