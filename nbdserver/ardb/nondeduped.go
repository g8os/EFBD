package ardb

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// newNonDedupedStorage returns the non deduped storage implementation
func newNonDedupedStorage(volumeID string, blockSize int64, provider *redisProvider) storage {
	return &nonDedupedStorage{
		blockSize: blockSize,
		volumeID:  volumeID,
		provider:  provider,
	}
}

// nonDedupedStorage is a storage implementation,
// that simply stores each block in redis using
// a unique key based on the volumeID and blockIndex
type nonDedupedStorage struct {
	blockSize int64
	volumeID  string
	provider  *redisProvider
}

// Set implements storage.Set
func (ss *nonDedupedStorage) Set(blockIndex int64, content []byte) (err error) {
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
func (ss *nonDedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
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
//  The length + offset should not exceed the blocksize
func (ss *nonDedupedStorage) MergeZeroes(blockIndex, offset, length int64) (err error) {

	content, err := ss.Get(blockIndex)
	if err != nil {
		return
	}

	//If the original content does not exist, no need to fill it with 0's
	if content == nil {
		return
	}
	// Assume the length of the original content == blocksize
	for i := offset; i < offset+length; i++ {
		content[i] = 0
	}
	// store new content
	err = ss.Set(blockIndex, content)
	return
}

// Get implements storage.Get
func (ss *nonDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
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
func (ss *nonDedupedStorage) Delete(blockIndex int64) (err error) {
	conn := ss.provider.GetRedisConnection(int(blockIndex))
	defer conn.Close()

	key := ss.getKey(blockIndex)
	_, err = conn.Do("DEL", key)
	return
}

// Flush implements storage.Flush
func (ss *nonDedupedStorage) Flush() (err error) {
	// nothing to do for the nonDeduped Storage
	return
}

// get the unique key for a block,
// based on its index and the shared volumeID
func (ss *nonDedupedStorage) getKey(blockIndex int64) string {
	//Is twice as fast as fmt.Sprintf
	return ss.volumeID + ":" + strconv.Itoa(int(blockIndex))
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
