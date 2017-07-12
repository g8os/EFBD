package copyvdisk

import (
	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
)

// NOTE: copies bitmask only
func copySemiDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (hasBitMask bool, err error) {
	script := redis.NewScript(0, `
local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
    return 0
end

if redis.call("EXISTS", destination) == 1 then
    redis.call("DEL", destination)
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))
return 1
`)

	log.Infof("dumping vdisk %q's bitmask and restoring it as vdisk %q's bitmask",
		sourceID, targetID)

	sourceKey := ardb.SemiDedupBitMapKey(sourceID)
	targetKey := ardb.SemiDedupBitMapKey(targetID)

	hasBitMask, err = redis.Bool(script.Do(conn, sourceKey, targetKey))
	return
}

// NOTE: copies bitmask only
func copySemiDedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (hasBitMask bool, err error) {
	sourceKey := ardb.SemiDedupBitMapKey(sourceID)

	log.Infof("collecting semidedup bitmask from source vdisk %q...", sourceID)
	bytes, err := redis.Bytes(connA.Do("GET", sourceKey))
	if err == redis.ErrNil {
		err = nil
		log.Infof("no semidedup bitmask found for source vdisk %q...", sourceID)
		return // nothing to do, as there is no bitmask
	}
	if err != nil {
		return // couldn't get bitmask due to an unexpected error
	}

	log.Infof("collected semidedup bitmask from source vdisk %q...", sourceID)

	targetKey := ardb.SemiDedupBitMapKey(targetID)
	_, err = connB.Do("SET", targetKey, bytes)
	if err != nil {
		return // couldn't set bitmask, this makes the vdisk invalid
	}

	log.Infof("stored semidedup bitmask for target vdisk %q...", targetID)

	hasBitMask = true
	return
}
