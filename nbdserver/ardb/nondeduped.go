package ardb

import (
	"context"
	"strconv"

	log "github.com/glendc/go-mini-log"

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
	key := ss.getKey(blockIndex)

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
		_, err = conn.Do("DEL", key)
		return
	}

	// content is not zero, so let's (over)write it
	_, err = conn.Do("SET", key, content)
	return
}

// Merge implements backendStorage.Merge
func (ss *nonDedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	key := ss.getKey(blockIndex)

	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	origContent, _ := redis.Bytes(conn.Do("GET", key))
	if ocl := int64(len(origContent)); ocl == 0 {
		origContent = make([]byte, ss.blockSize)
	} else if ocl < ss.blockSize {
		oc := make([]byte, ss.blockSize)
		copy(oc, origContent)
		origContent = oc
	}

	// copy in new content
	copy(origContent[offset:], content)

	// store new content, as the merged version is non-zero
	_, err = conn.Do("SET", key, origContent)
	return
}

// Get implements backendStorage.Get
func (ss *nonDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	key := ss.getKey(blockIndex)

	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	content, err = redisBytes(conn.Do("GET", key))
	return
}

// Delete implements backendStorage.Delete
func (ss *nonDedupedStorage) Delete(blockIndex int64) (err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	key := ss.getKey(blockIndex)
	_, err = conn.Do("DEL", key)
	return
}

// Flush implements backendStorage.Flush
func (ss *nonDedupedStorage) Flush() (err error) {
	// nothing to do for the nonDeduped backendStorage
	return
}

// Close implements backendStorage.Close
func (ss *nonDedupedStorage) Close() error {
	return nil // nothing to close
}

// GoBackground implements backendStorage.GoBackground
func (ss *nonDedupedStorage) GoBackground(ctx context.Context) {
	// no background thread needed
}

// get the unique key for a block,
// based on its index and the shared vdiskID
func (ss *nonDedupedStorage) getKey(blockIndex int64) string {
	//Is twice as fast as fmt.Sprintf
	return ss.vdiskID + ":" + strconv.FormatInt(blockIndex, 10)
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ss *nonDedupedStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}
