package copyvdisk

import (
	"github.com/g8os/blockstor/log"
	"github.com/garyburd/redigo/redis"
)

// get 1 or 2 ardb connection(s) based on the given input
func getARDBConnections(input *userInputPair, sourcedb, targetdb int) (connA, connB redis.Conn, err error) {
	// dial first (source) connection string
	connA, err = redis.Dial("tcp", input.Source.URL, redis.DialDatabase(sourcedb))
	if err != nil {
		return // early return if exit
	}
	if input.Source.URL == input.Target.URL && sourcedb == targetdb {
		log.Infof("only 1 TCP connection is required: %s@%d", input.Source.URL, sourcedb)
		return
	}

	// dial second (target) connection string
	connB, err = redis.Dial("tcp", input.Target.URL, redis.DialDatabase(targetdb))
	if err != nil {
		connA.Close()
		connA = nil // reset connA again
		return      // early return if exit
	}
	log.Infof("2 TCP connections required: %s@%d => %s@%d",
		input.Source.URL, sourcedb, input.Target.URL, targetdb)
	return
}
