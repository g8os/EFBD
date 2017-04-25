package main

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func copyDifferentConnections(logger log.Logger, input *userInputPair, connA, connB redis.Conn) (err error) {
	defer func() {
		connA.Close()
		connB.Close()
	}()

	// get data from source connection
	logger.Infof("collecting all metadata from source vdisk %q...", input.Source.VdiskID)
	data, err := redis.StringMap(connA.Do("HGETALL", input.Source.VdiskID))
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("%q does not exist", input.Source.VdiskID)
		return
	}
	logger.Infof("collected %d meta indices from source vdisk %q",
		len(data), input.Source.VdiskID)

	// ensure the vdisk isn't touched while we're creating it
	if err = connB.Send("WATCH", input.Target.VdiskID); err != nil {
		return
	}

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", input.Target.VdiskID); err != nil {
		return
	}

	// buffer all data on target connection
	logger.Infof("buffering %d meta indices for target vdisk %q...",
		len(data), input.Target.VdiskID)
	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", input.Target.VdiskID, index, []byte(hash))
	}

	// send all data to target connection (execute the transaction)
	logger.Infof("flushing buffered metadata for target vdisk %q...", input.Target.VdiskID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", input.Target.VdiskID)
	}

	return
}
