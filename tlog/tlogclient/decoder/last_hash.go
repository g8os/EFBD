package decoder

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor/tlog"
)

var (
	// ErrNilLastHash indicates that there is no last hash entry
	// in the metadata storage.
	ErrNilLastHash = errors.New("nil last hash")
)

// GetLastHash returns valid last hash of a vdisk.
func GetLastHash(rc redis.Conn, vdiskID string) ([]byte, error) {
	key := tlog.LastHashPrefix + vdiskID

	hashes, err := redis.ByteSlices(rc.Do("LRANGE", key, 0, -1))
	if err == redis.ErrNil {
		return nil, ErrNilLastHash
	}

	if err != nil {
		return nil, err
	}

	if len(hashes) == 0 {
		return nil, ErrNilLastHash
	}

	// check that the hash really valid
	for _, hash := range hashes {
		if _, err := rc.Do("GET", hash); err == nil {
			return hash, nil
		}
	}

	return nil, fmt.Errorf("no valid hash for vdiskID: %s", vdiskID)
}
