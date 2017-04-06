if #ARGV ~= 2 then
    local usage = "copyvolume usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copyvolume requires 2 arguments (source destination)")
end

local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", destination) == 1 then
  if type(ARGV[1]) == "string" and ARGV[1]:upper() == "NX" then
    return nil
  else
    redis.call("DEL", destination)
  end
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))
return "OK"
