package main

import (
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func getConnectionsFromMetaServer(logger log.Logger, input *userInput) (connA, connB redis.Conn, err error) {
	connA, err = redis.Dial("tcp", input.Source.URL)
	if err != nil {
		return
	}
	if input.Target.URL == input.Source.URL {
		logger.Info("only 1 TCP connection is required:", input.Source.URL)
		return
	}

	logger.Infof("2 TCP connections required: %q => %q",
		input.Source.URL, input.Target.URL)
	connB, err = redis.Dial("tcp", input.Target.URL)
	return
}
