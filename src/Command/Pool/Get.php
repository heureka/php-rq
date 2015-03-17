<?php

namespace PhpRQ\Command\Pool;

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
local size = ARGV[1]
local time = ARGV[2]
local ackTTL = ARGV[3]

local result = redis.call('zrangebyscore', pool, '-inf', time, 'WITHSCORES', 'LIMIT', 0, size)
local finalResult = {}
local i
local value
local score
for i = 1, #result, 2 do
    value = result[i]
    score = math.floor(result[i + 1])
    redis.call('zadd', pool, score + tonumber(ackTTL) + 0.1, value)
    table.insert(finalResult, value)
end

return finalResult
LUA;
    }
}
