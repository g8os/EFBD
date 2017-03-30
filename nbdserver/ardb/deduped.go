package ardb

import (
	"fmt"

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

	//Execute in a function so the redis connection is released before storing the hash in the LBA
	// If the metadataserver is the same as the content, this causes a deadlock if the connection is not released yet
	// and now it can already be reused faster as well
	err = func() (err error) {
		conn := ds.getRedisConnection(hash)
		defer conn.Close()

		exists, err := redis.Bool(conn.Do("EXISTS", hash))
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

	var mergedContent []byte

	if hash != nil {
		mergedContent, err = ds.getContent(hash)
		if err != nil {
			err = fmt.Errorf("LBA hash refered to non-existing content: %s", err)
			return
		}
		if int64(len(mergedContent)) < ds.blockSize {
			mc := make([]byte, ds.blockSize)
			copy(mc, mergedContent)
			mergedContent = mc
		}
	} else {
		mergedContent = make([]byte, ds.blockSize)
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content
	return ds.Set(blockIndex, mergedContent)
}

// MergeZeroes implements storage.MergeZeroes
func (ds *dedupedStorage) MergeZeroes(blockIndex, offset, length int64) (err error) {
	hash, _ := ds.lba.Get(blockIndex)
	if hash == nil {
		return
	}

	origContent, _ := ds.getContent(hash)
	origLength := int64(len(origContent))
	if origLength < ds.blockSize {
		oc := make([]byte, ds.blockSize)
		copy(oc, origContent)
		origContent = oc
	}

	// copy in zero content
	zeroLength := ds.blockSize - offset
	if zeroLength > length {
		zeroLength = length
	}
	copy(origContent[offset:], make([]byte, length))

	// store new content
	return ds.Set(blockIndex, origContent)
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

// Delete implements storage.Delete
func (ds *dedupedStorage) Delete(blockIndex int64) (err error) {
	err = ds.lba.Delete(blockIndex)
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
