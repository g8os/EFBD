package ardb

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor/nbdserver/lba"
)

// newDedupedStorage returns the deduped backendStorage implementation
func newDedupedStorage(vdiskID string, blockSize int64, provider redisConnectionProvider, vlba *lba.LBA) backendStorage {
	return &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: lba.HashBytes(make([]byte, blockSize)),
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
	zeroContentHash lba.Hash
	provider        redisConnectionProvider
	lba             *lba.LBA
}

// Set implements backendStorage.Set
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
		conn, err := ds.getRedisConnection(hash)
		if err != nil {
			return
		}
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

// Merge implements backendStorage.Merge
func (ds *dedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	hash, _ := ds.lba.Get(blockIndex)

	var mergedContent []byte

	if hash != nil && !hash.Equals(lba.NilHash) {
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

// Get implements backendStorage.Get
func (ds *dedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	hash, err := ds.lba.Get(blockIndex)
	if err == nil && hash != nil && !hash.Equals(lba.NilHash) {
		content, err = ds.getContent(hash)
	}
	return
}

// Delete implements backendStorage.Delete
func (ds *dedupedStorage) Delete(blockIndex int64) (err error) {
	err = ds.lba.Delete(blockIndex)
	return
}

// Flush implements backendStorage.Flush
func (ds *dedupedStorage) Flush() (err error) {
	err = ds.lba.Flush()
	return
}

func (ds *dedupedStorage) getRedisConnection(hash lba.Hash) (redis.Conn, error) {
	return ds.provider.RedisConnection(int64(hash[0]))
}

func (ds *dedupedStorage) getFallbackRedisConnection(hash lba.Hash) (redis.Conn, error) {
	return ds.provider.FallbackRedisConnection(int64(hash[0]))
}

// getContent from the local storage,
// if the content can't be found locally, we'll try to fetch it from the root (remote) storage.
// if the content is available in the remote storage,
// we'll also try to store it in the local storage before returning that content
func (ds *dedupedStorage) getContent(hash lba.Hash) (content []byte, err error) {
	// try to fetch it from the local storage
	content, err = func() (content []byte, err error) {
		conn, err := ds.getRedisConnection(hash)
		if err != nil {
			return
		}
		defer conn.Close()

		content, err = redisBytes(conn.Do("GET", hash))
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

		content, err = redis.Bytes(conn.Do("GET", hash))
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
			log.Debugf(
				"storing remote content for %v in local storage (asynchronously)",
				hash)
			conn, err := ds.getRedisConnection(hash)
			if err != nil {
				return
			}

			_, err = conn.Do("SET", hash, content)
			if err != nil {
				// we won't return error however, but just log it
				log.Infof("couldn't store remote content in local storage: %s", err.Error())
			}
		}()

		log.Debugf(
			"no local content available for %v, but did find it as remote content",
			hash)
	}

	// err = nil, content = ?
	return
}
