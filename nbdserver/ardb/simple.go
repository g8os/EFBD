package ardb

import (
	"strconv"

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

// MergeZeroes implements storage.MergeZeroes
func (ss *simpleStorage) MergeZeroes(blockIndex, offset, length int64) (err error) {
	key := ss.getKey(blockIndex)

	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	origContent, _ := redis.Bytes(conn.Do("GET", key))

	if ocl := int64(len(origContent)); ocl == 0 {
		origContent = make([]byte, ss.blockSize)
	} else if ocl < ss.blockSize {
		oc := make([]byte, ss.blockSize)
		copy(oc, origContent)
		origContent = oc
	}

	// copy in zero content
	zeroLength := ss.blockSize - offset
	if zeroLength > length {
		zeroLength = length
	}
	copy(origContent[offset:], make([]byte, zeroLength))

	// store new content, as the merged version is non-zero
	_, err = conn.Do("SET", key, origContent)
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

// Delete implements storage.Delete
func (ss *simpleStorage) Delete(blockIndex int64) (err error) {
	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	key := ss.getKey(blockIndex)
	_, err = conn.Do("DEL", key)
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
	//Is twice as fast as fmt.Sprintf
	return ss.volumeID + ":" + strconv.Itoa(int(blockIndex))
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
