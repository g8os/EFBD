if #ARGV ~= 2 then
    local usage = "copyvolume usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copyvolume requires 2 arguments (source destination)")
end

local source = ARGV[1]
-- all shard keys start with `<VOLUME>:`
local destination = ARGV[2] .. ':'

local shards = redis.call('keys', source .. ':*')

local shardIndex
local setResult
local srcKey
local dstKey
local shard

local numberOfShards = #shards

-- go through all shards of source volume
for shardCount = 1, numberOfShards do
    srcKey = shards[shardCount]

    -- get shard
    shard = redis.call('get', srcKey)
    if shard == nil then
        error("couldn't get shard: " .. srcKey)
    end

    -- validate srcKey & extract shardIndex
    shardIndex = string.match(srcKey, "^[^:]+:(%d+)$")
    if shardIndex == nil then
        error(srcKey .. " is an invalid shard name")
    end

    -- set shard
    dstKey = destination .. shardIndex
    setResult = redis.call('set', dstKey, shard)
    if setResult == nil then
        error("couldn't copy shard " .. srcKey .. " to " .. dstKey)
    end
end

-- success, return amount of shards copied
return numberOfShards