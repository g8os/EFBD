package ardb

import (
	"errors"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// shared constants
const (
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * MebibyteAsBytes // 20 MiB
	// constants used to convert between MiB/GiB and bytes
	GibibyteAsBytes int64 = 1024 * 1024 * 1024
	MebibyteAsBytes int64 = 1024 * 1024
)

// GetConnection gets an ardb connection given a storage server config
func GetConnection(cfg config.StorageServerConfig) (redis.Conn, error) {
	return redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
}

// GetConnections gets multiple ardb connections given storage server configs
func GetConnections(cfgs ...config.StorageServerConfig) (conns []redis.Conn, err error) {
	if len(cfgs) == 0 {
		return nil, errors.New("no storage server configs given")
	}

	var conn redis.Conn
	for _, cfg := range cfgs {
		// get connection
		conn, err = GetConnection(cfg)
		if err != nil {
			// connecton failed, close all open connections and return it all
			var closeErr error
			for _, conn = range conns {
				closeErr = conn.Close()
				if closeErr != nil {
					log.Errorf("couldn't close open connection: %v", err)
				}
			}

			return
		}

		// connection established, add it to the list of open connections
		conns = append(conns, conn)
	}

	return
}

// RedisBytes is a utility function used by BlockStorage implementations,
// where we don't want to trigger an error for non-existent (or nil) content.
func RedisBytes(reply interface{}, replyErr error) (content []byte, err error) {
	content, err = redis.Bytes(reply, replyErr)
	// This could happen in case the block doesn't exist,
	// or in case the block is a nil block.
	// in both cases we want to simply return it as a nil block.
	if err == redis.ErrNil {
		err = nil
	}

	return
}
