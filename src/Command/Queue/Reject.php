<?php
/**
 * Copyright (c) 2021 Heureka Group a.s. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *limitations under the License.
 */
namespace PhpRQ\Command\Queue;

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
local item = ARGV[1]

local removed = redis.call('lrem', processing, -1, item)

if removed == 1 then
    redis.call('rpush', queue, item)
end

local count = redis.call('llen', processing)
if count == 0 then
    redis.call('hdel', timeouts, processing)
end
LUA;
    }

}
