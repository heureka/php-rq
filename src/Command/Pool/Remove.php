<?php

namespace PhpRQ\Command\Pool;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class Remove extends \Predis\Command\ScriptCommand
{

    /**
     * @return int The number of the keys passed to the method as arguments
     */
    protected function getKeysCount()
    {
        return 1;
    }

    /**
     * Gets the body of a Lua script.
     *
     * @return string
     */
    public function getScript()
    {
        return <<<'LUA'
local pool = KEYS[1]
local item = ARGV[1]

local score = redis.call('zscore', pool, item)
if score and score - math.floor(score) > 0.01 then
    redis.call('zrem', pool, item)
end
LUA;
    }

}
