package ardb

import (
	"github.com/garyburd/redigo/redis"
)

// backendStorage defines the interface for the actual storage implementation,
// used by ArbdBackend for a particular vdisk
type backendStorage interface {
	Set(blockIndex int64, content []byte) (err error)
	Merge(blockIndex, offset int64, content []byte) (mergedContent []byte, err error)
	Get(blockIndex int64) (content []byte, err error)
	Delete(blockIndex int64) (err error)
	Flush() (err error)
}

// redisBytes is a utility function used by backendStorage functions,
// where we don't want to trigger an error for non-existent (or null) content.
func redisBytes(reply interface{}, replyErr error) (content []byte, err error) {
	content, err = redis.Bytes(reply, replyErr)
	// This could happen in case the block doesn't exist,
	// or in case the block is a nullblock.
	// in both cases we want to simply return it as a null block.
	if err == redis.ErrNil {
		err = nil
	}

	return
}
