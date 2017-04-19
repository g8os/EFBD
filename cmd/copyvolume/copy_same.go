package main

import (
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func copySameConnection(logger log.Logger, input *userInput, conn redis.Conn) (err error) {
	defer conn.Close()

	logger.Infof("dumping volume %q and restoring it as volume %q",
		input.Source.Volume, input.Target.Volume)

	indexCount, err := redis.Int64(copySameScript.Do(conn,
		input.Source.Volume, input.Target.Volume))
	if err == nil {
		logger.Infof("copied %d meta indices to volume %q",
			indexCount, input.Target.Volume)
	}

	return
}

var copySameScript = redis.NewScript(0, copySameScriptSource)

const copySameScriptSource = `
if #ARGV ~= 2 then
    local usage = "copyvolume usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copyvolume requires 2 arguments (source destination)")
end

local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
	return redis.error_reply('"' .. source .. '" does not exist')
end

if redis.call("EXISTS", destination) == 1 then
  if type(ARGV[1]) == "string" and ARGV[1]:upper() == "NX" then
    return nil
  else
    redis.call("DEL", destination)
  end
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))

return redis.call("HLEN", destination)
`
