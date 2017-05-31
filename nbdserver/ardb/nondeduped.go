package ardb

import (
	"context"

	"github.com/g8os/blockstor/log"

	"github.com/garyburd/redigo/redis"
)

// newNonDedupedStorage returns the non deduped backendStorage implementation
func newNonDedupedStorage(vdiskID string, blockSize int64, provider redisConnectionProvider) backendStorage {
	return &nonDedupedStorage{
		blockSize: blockSize,
		vdiskID:   vdiskID,
		provider:  provider,
	}
}

// nonDedupedStorage is a backendStorage implementation,
// that simply stores each block in redis using
// a unique key based on the vdiskID and blockIndex
type nonDedupedStorage struct {
	blockSize int64
	vdiskID   string
	provider  redisConnectionProvider
}

// Set implements backendStorage.Set
func (ss *nonDedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ss.isZeroContent(content) {
		log.Debugf(
			"deleting content @ %d for vdisk %s as it's an all zeroes block",
			blockIndex, ss.vdiskID)
		_, err = conn.Do("HDEL", ss.vdiskID, blockIndex)
		return
	}

	// content is not zero, so let's (over)write it
	_, err = conn.Do("HSET", ss.vdiskID, blockIndex, content)
	return
}

// Merge implements backendStorage.Merge
func (ss *nonDedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	mergedContent, _ := redis.Bytes(conn.Do("HGET", ss.vdiskID, blockIndex))
	if ocl := int64(len(mergedContent)); ocl == 0 {
		mergedContent = make([]byte, ss.blockSize)
	} else if ocl < ss.blockSize {
		oc := make([]byte, ss.blockSize)
		copy(oc, mergedContent)
		mergedContent = oc
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content, as the merged version is non-zero
	_, err = conn.Do("HSET", ss.vdiskID, blockIndex, mergedContent)
	return
}

// Get implements backendStorage.Get
func (ss *nonDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	content, err = redisBytes(conn.Do("HGET", ss.vdiskID, blockIndex))
	return
}

// Delete implements backendStorage.Delete
func (ss *nonDedupedStorage) Delete(blockIndex int64) (err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("HDEL", ss.vdiskID, blockIndex)
	return
}

// Flush implements backendStorage.Flush
func (ss *nonDedupedStorage) Flush() (err error) {
	// nothing to do for the nonDeduped backendStorage
	return
}

// Close implements backendStorage.Close
func (ss *nonDedupedStorage) Close() error { return nil }

// GoBackground implements backendStorage.GoBackground
func (ss *nonDedupedStorage) GoBackground(context.Context) {}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ss *nonDedupedStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}
