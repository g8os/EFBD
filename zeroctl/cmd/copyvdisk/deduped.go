package copyvdisk

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
)

func copyDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (err error) {
	defer conn.Close()

	script := redis.NewScript(0, `
local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
    return redis.error_reply('"' .. source .. '" does not exist')
end

if redis.call("EXISTS", destination) == 1 then
    redis.call("DEL", destination)
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))

return redis.call("HLEN", destination)
`)

	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	indexCount, err := redis.Int64(script.Do(conn, sourceID, targetID))
	if err == nil {
		log.Infof("copied %d meta indices to vdisk %q", indexCount, targetID)
	}

	return
}

func copyDedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (err error) {
	defer func() {
		connA.Close()
		connB.Close()
	}()

	// get data from source connection
	log.Infof("collecting all metadata from source vdisk %q...", sourceID)
	data, err := redis.StringMap(connA.Do("HGETALL", sourceID))
	if err != nil {
		return
	}
	dataLength := len(data)
	if dataLength == 0 {
		err = fmt.Errorf("%q does not exist", sourceID)
		return
	}

	log.Infof("collected %d meta indices from source vdisk %q",
		dataLength, sourceID)

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", targetID); err != nil {
		return
	}

	// buffer all data on target connection
	log.Infof("buffering %d meta indices for target vdisk %q...",
		len(data), targetID)
	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", targetID, index, []byte(hash))
	}

	// send all data to target connection (execute the transaction)
	log.Infof("flushing buffered metadata for target vdisk %q...", targetID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", targetID)
	}

	return
}
