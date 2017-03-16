package arbd

import (
	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor/nbdserver/lba"
)

// newDedupedStorage returns the deduped storage implementation
func newDedupedStorage(volumeID string, blockSize int64, provider *redisProvider, vlba *lba.LBA) storage {
	return &dedupedStorage{
		blockSize:       blockSize,
		volumeID:        volumeID,
		zeroContentHash: lba.HashBytes(make([]byte, blockSize)),
		provider:        provider,
		lba:             vlba,
	}
}

// dedupedStorage is a storage implementation,
// that stores the content based on a hash unique to that content,
// all hashes are linked to the volume using lba.LBA
type dedupedStorage struct {
	blockSize       int64
	volumeID        string
	zeroContentHash lba.Hash
	provider        *redisProvider
	lba             *lba.LBA
}

// Set implements storage.Set
func (ds *dedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	hash := lba.HashBytes(content)
	if ds.zeroContentHash.Equals(hash) {
		err = ds.lba.Delete(blockIndex)
		return
	}

	err = func() (err error) {
		conn := ds.getRedisConnection(hash)
		defer conn.Close()

		var exists bool
		exists, err = redis.Bool(conn.Do("EXISTS", hash))
		if err != nil || exists {
			return
		}

		// write content to redis in case it doesn't exist yet
		_, err = conn.Do("SET", hash, content)
		return
	}()
	if err != nil {
		return
	}

	// Write Hash to LBA

	err = ds.lba.Set(blockIndex, hash)
	return
}

// Merge implements storage.Merge
func (ds *dedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	hash, _ := ds.lba.Get(blockIndex)
	mergedContent := make([]byte, ds.blockSize)

	if hash != nil {
		// copy original content
		origContent, _ := ds.getContent(hash)
		copy(mergedContent, origContent)
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content
	return ds.Set(blockIndex, mergedContent)
}

// Get implements storage.Get
func (ds *dedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	contentHash, err := ds.lba.Get(blockIndex)
	if err != nil || contentHash == nil {
		return
	}

	content, err = ds.getContent(contentHash)
	return
}

// Flush implements storage.Flush
func (ds *dedupedStorage) Flush() (err error) {
	err = ds.lba.Flush()
	return
}

func (ds *dedupedStorage) getRedisConnection(hash lba.Hash) (conn redis.Conn) {
	conn = ds.provider.GetRedisConnection(int(hash[0]))
	return
}

func (ds *dedupedStorage) getContent(hash lba.Hash) (content []byte, err error) {
	conn := ds.getRedisConnection(hash)
	defer conn.Close()

	content, err = redis.Bytes(conn.Do("GET", hash))
	// This could happen in case the block doesn't exist,
	// or in case the block is a nullblock.
	// in both cases we want to simply return it as a null block.
	if err == redis.ErrNil {
		err = nil
	}

	return
}
