<?php

namespace PhpRQ\Command\Queue;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Ack extends \Predis\Command\ScriptCommand
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
local processing = KEYS[1]
local timeouts = KEYS[2]
local item = ARGV[1]

local result = redis.call('lrem', processing, -1, item)

local count = redis.call('llen', processing)
if count == 0 then
    redis.call('hdel', timeouts, processing)
end
LUA;
    }

}
