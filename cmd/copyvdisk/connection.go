package main

import (
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

// get 1 or 2 ardb connection(s) based on the given input
func getARDBConnections(logger log.Logger, input *userInputPair) (connA, connB redis.Conn, err error) {
	// dial first (source) connection string
	connA, err = redis.Dial("tcp", input.Source.URL)
	if err != nil {
		return // early return if exit
	}
	if input.Source.URL == input.Target.URL {
		logger.Info("only 1 TCP connection is required:", input.Source.URL)
		return
	}

	// dial second (target) connection string
	connB, err = redis.Dial("tcp", input.Target.URL)
	if err != nil {
		connA.Close()
		connA = nil // reset connA again
		return      // early return if exit
	}
	logger.Infof("2 TCP connections required: %q => %q", input.Source.URL, input.Target.URL)
	return
}
