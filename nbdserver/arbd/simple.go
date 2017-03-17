package arbd

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// newSimpleStorage returns the simple storage implementation
func newSimpleStorage(volumeID string, blockSize int64, provider *redisProvider) storage {
	return &simpleStorage{
		blockSize: blockSize,
		volumeID:  volumeID,
		provider:  provider,
	}
}

// simpleStorage is a storage implementation,
// that simply stores each block in redis using
// a unique key based on the volumeID and blockIndex
type simpleStorage struct {
	blockSize int64
	volumeID  string
	provider  *redisProvider
}

// Set implements storage.Set
func (ss *simpleStorage) Set(blockIndex int64, content []byte) (err error) {
	key := ss.getKey(blockIndex)

	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ss.isZeroContent(content) {
		_, err = conn.Do("DEL", key)
		return
	}

	// content is not zero, so let's (over)write it
	_, err = conn.Do("SET", key, content)
	return
}

// Merge implements storage.Merge
func (ss *simpleStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	key := ss.getKey(blockIndex)

	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	mergedContent := make([]byte, ss.blockSize)

	if oc, _ := redis.Bytes(conn.Do("GET", key)); len(oc) > 0 {
		// copy original content
		copy(mergedContent, oc)
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// don't store zero blocks,
	// and delete the block if it previously existed
	if ss.isZeroContent(mergedContent) {
		_, err = conn.Do("DEL", key)
		return
	}

	// store new content, as the merged version is non-zero
	_, err = conn.Do("SET", key, mergedContent)
	return
}

// Get implements storage.Get
func (ss *simpleStorage) Get(blockIndex int64) (content []byte, err error) {
	key := ss.getKey(blockIndex)

	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	content, err = redis.Bytes(conn.Do("GET", key))
	// This could happen in case the block doesn't exist,
	// or in case the block is a nullblock.
	// in both cases we want to simply return it as a null block.
	if err == redis.ErrNil {
		err = nil
	}

	return
}

// Flush implements storage.Flush
func (ss *simpleStorage) Flush() (err error) {
	err = nil // not required for the simple storage
	return
}

// get the unique key for a block,
// based on its index and the shared volumeID
func (ss *simpleStorage) getKey(blockIndex int64) string {
	return fmt.Sprintf("%s:%d", ss.volumeID, blockIndex)
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ss *simpleStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}
