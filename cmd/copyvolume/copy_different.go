package main

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func copyDifferentConnections(logger log.Logger, input *userInput, connA, connB redis.Conn) (err error) {
	defer func() {
		connA.Close()
		connB.Close()
	}()

	// get data from source connection
	logger.Infof("collecting all metadata from source volume %q...", input.Source.Volume)
	data, err := redis.StringMap(connA.Do("HGETALL", input.Source.Volume))
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("%q does not exist", input.Source.Volume)
		return
	}
	logger.Infof("collected %d meta indices from source volume %q",
		len(data), input.Source.Volume)

	// buffer all data on target connection
	logger.Infof("buffering %d meta indices for target volume %q...",
		len(data), input.Target.Volume)
	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", input.Target.Volume, index, []byte(hash))
	}

	// send all data to target connection
	logger.Infof("flushing buffered metadata for target volume %q...", input.Target.Volume)
	err = connB.Flush()
	return
}
