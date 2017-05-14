package ardb

import (
	"fmt"

	"github.com/g8os/blockstor/log"
	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/nbdserver/lba"
)

// newDedupedStorage returns the deduped backendStorage implementation
func newDedupedStorage(vdiskID string, blockSize int64, provider redisConnectionProvider, vlba *lba.LBA) backendStorage {
	return &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: blockstor.HashBytes(make([]byte, blockSize)),
		provider:        provider,
		lba:             vlba,
	}
}

// dedupedStorage is a backendStorage implementation,
// that stores the content based on a hash unique to that content,
// all hashes are linked to the vdisk using lba.LBA
type dedupedStorage struct {
	blockSize       int64
	vdiskID         string
	zeroContentHash blockstor.Hash
	provider        redisConnectionProvider
	lba             *lba.LBA
}

// Set implements backendStorage.Set
func (ds *dedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	hash := blockstor.HashBytes(content)
	if ds.zeroContentHash.Equals(hash) {
		log.Debugf(
			"deleting hash @ %d from LBA for deduped vdisk %s as it's an all zeroes block",
			blockIndex, ds.vdiskID)
		err = ds.lba.Delete(blockIndex)
		return
	}

	// reference the content to this vdisk,
	// and set the content itself, if it didn't exist yet
	_, err = ds.setContent(hash, content)
	if err != nil {
		return
	}

	return ds.lba.Set(blockIndex, hash)
}

// Merge implements backendStorage.Merge
func (ds *dedupedStorage) Merge(blockIndex, offset int64, content []byte) (mergedContent []byte, err error) {
	hash, _ := ds.lba.Get(blockIndex)

	if hash != nil && !hash.Equals(blockstor.NilHash) {
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
	// (dereferencing of previousHash happens in ds.Set logic)
	err = ds.Set(blockIndex, mergedContent)
	return
}

// Get implements backendStorage.Get
func (ds *dedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	hash, err := ds.lba.Get(blockIndex)
	if err == nil && hash != nil && !hash.Equals(blockstor.NilHash) {
		content, err = ds.getContent(hash)
	}
	return
}

// Delete implements backendStorage.Delete
func (ds *dedupedStorage) Delete(blockIndex int64) (err error) {
	// first get hash
	hash, _ := ds.lba.Get(blockIndex)
	if hash == nil {
		// content didn't exist yet,
		// so we've nothing to do here
		return
	}

	// delete the actual hash from the LBA
	err = ds.lba.Delete(blockIndex)
	return
}

// Flush implements backendStorage.Flush
func (ds *dedupedStorage) Flush() (err error) {
	err = ds.lba.Flush()
	return
}

func (ds *dedupedStorage) getRedisConnection(hash blockstor.Hash) (redis.Conn, error) {
	return ds.provider.RedisConnection(int64(hash[0]))
}

func (ds *dedupedStorage) getFallbackRedisConnection(hash blockstor.Hash) (redis.Conn, error) {
	return ds.provider.FallbackRedisConnection(int64(hash[0]))
}

// getContent from the local storage,
// if the content can't be found locally, we'll try to fetch it from the root (remote) storage.
// if the content is available in the remote storage,
// we'll also try to store it in the local storage before returning that content
func (ds *dedupedStorage) getContent(hash blockstor.Hash) (content []byte, err error) {
	// try to fetch it from the local storage
	content, err = func() (content []byte, err error) {
		conn, err := ds.getRedisConnection(hash)
		if err != nil {
			return
		}
		defer conn.Close()

		content, err = redisBytes(conn.Do("GET", hash.Bytes()))
		return
	}()
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	// try to fetch it from the remote storage if available
	content = func() (content []byte) {
		conn, err := ds.getFallbackRedisConnection(hash)
		if err != nil {
			log.Debugf(
				"no local content available for %v and no remote storage available: %s",
				hash, err.Error())
			return
		}
		defer conn.Close()

		content, err = redisBytes(conn.Do("GET", hash.Bytes()))
		if err != nil {
			log.Debugf(
				"content for %v not available in local-, nor in remote storage: %s",
				hash, err.Error())
		}

		return
	}()

	if content != nil {
		// store remote content in local storage asynchronously
		go func() {
			success, err := ds.setContent(hash, content)
			if err != nil {
				// we won't return error however, but just log it
				log.Infof("couldn't store remote content in local storage: %s", err.Error())
			} else if success {
				log.Debugf(
					"stored remote content for %v in local storage (asynchronously)",
					hash)
			}
		}()

		log.Debugf(
			"no local content available for %v, but did find it as remote content",
			hash)
	}

	// err = nil, content = ?
	return
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash blockstor.Hash, content []byte) (success bool, err error) {
	conn, err := ds.getRedisConnection(hash)
	if err != nil {
		return
	}
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("EXISTS", hash.Bytes()))
	if err == nil && !exists {
		_, err = conn.Do("SET", hash.Bytes(), content)
		success = err == nil
	}

	return
}
