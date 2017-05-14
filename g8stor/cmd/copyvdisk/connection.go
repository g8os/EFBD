package copyvdisk

import (
	"github.com/g8os/blockstor/log"
	"github.com/garyburd/redigo/redis"
)

// get 1 or 2 ardb connection(s) based on the given input
func getARDBConnections(input *userInputPair, logger log.Logger) (connA, connB redis.Conn, err error) {
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
