package main

import (
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func copySameConnection(logger log.Logger, input *userInputPair, conn redis.Conn) (err error) {
	defer conn.Close()

	logger.Infof("dumping vdisk %q and restoring it as vdisk %q",
		input.Source.VdiskID, input.Target.VdiskID)

	indexCount, err := redis.Int64(copySameScript.Do(conn,
		input.Source.VdiskID, input.Target.VdiskID))
	if err == nil {
		logger.Infof("copied %d meta indices to vdisk %q",
			indexCount, input.Target.VdiskID)
	}

	return
}

var copySameScript = redis.NewScript(0, copySameScriptSource)

const copySameScriptSource = `
if #ARGV ~= 2 then
    local usage = "copySameScript usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copySameScript requires 2 arguments (source destination)")
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
