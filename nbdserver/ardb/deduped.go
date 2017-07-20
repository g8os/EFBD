package ardb

import (
	"context"
	"fmt"

	"github.com/garyburd/redigo/redis"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

// newDedupedStorage returns the deduped backendStorage implementation
func newDedupedStorage(vdiskID string, blockSize int64, provider redisDataConnProvider, templateSupport bool, vlba *lba.LBA) backendStorage {
	dedupedStorage := &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: zerodisk.HashBytes(make([]byte, blockSize)),
		provider:        provider,
		lba:             vlba,
	}

	// getContent is ALWAYS defined,
	// but the actual function used depends on
	// whether or not this storage has template support.
	if templateSupport {
		dedupedStorage.getContent = dedupedStorage.getPrimaryOrTemplateContent
	} else {
		dedupedStorage.getContent = dedupedStorage.getPrimaryContent
	}

	return dedupedStorage
}

// dedupedStorage is a backendStorage implementation,
// that stores the content (the data) based on a hash unique to that content,
// all hashes are linked to the vdisk using lba.LBA (the metadata).
// The metadata and data are stored on seperate servers.
// Accessing data is only ever possible by checking the metadata first.
type dedupedStorage struct {
	blockSize       int64                 // block size in bytes
	vdiskID         string                // ID of the vdisk
	zeroContentHash zerodisk.Hash         // a hash of a nil-block of blockSize
	provider        redisDataConnProvider // used to get a connection to a storage server
	lba             *lba.LBA              // the LBA used to get/set/modify the metadata (content hashes)
	getContent      dedupedContentGetter  // getContent function used to get content, is always defined
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type dedupedContentGetter func(hash zerodisk.Hash) (content []byte, err error)

// Set implements backendStorage.Set
func (ds *dedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	hash := zerodisk.HashBytes(content)
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
func (ds *dedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	hash, _ := ds.lba.Get(blockIndex)

	var mergedContent []byte
	if hash != nil && !hash.Equals(zerodisk.NilHash) {
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
	if err == nil && hash != nil && !hash.Equals(zerodisk.NilHash) {
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

func (ds *dedupedStorage) getRedisConnection(hash zerodisk.Hash) (redis.Conn, error) {
	return ds.provider.RedisConnection(int64(hash[0]))
}

func (ds *dedupedStorage) getTemplateRedisConnection(hash zerodisk.Hash) (redis.Conn, error) {
	return ds.provider.TemplateRedisConnection(int64(hash[0]))
}

// getPrimaryContent gets content from the primary storage.
// Assigned to (*dedupedStorage).getContent in case this storage has no template support.
func (ds *dedupedStorage) getPrimaryContent(hash zerodisk.Hash) (content []byte, err error) {
	conn, err := ds.getRedisConnection(hash)
	if err != nil {
		return
	}
	defer conn.Close()

	content, err = redisBytes(conn.Do("GET", hash.Bytes()))
	return
}

// getPrimaryOrTemplateContent gets content from the primary storage,
// or if the content can't be found in primary storage,
// we'll try to fetch it from the template storage.
// if the content is available in the template storage,
// we'll also try to store it in the primary storage before returning that content.
// Assigned to (*dedupedStorage).getContent in case this storage has template support.
func (ds *dedupedStorage) getPrimaryOrTemplateContent(hash zerodisk.Hash) (content []byte, err error) {
	// try to fetch it from the primary storage
	content, err = ds.getPrimaryContent(hash)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	// try to fetch it from the template storage if available
	content = func() (content []byte) {
		conn, err := ds.getTemplateRedisConnection(hash)
		if err != nil {
			log.Debugf(
				"content not available in primary storage for %v and no template storage available: %s",
				hash, err.Error())
			return
		}
		defer conn.Close()

		content, err = redisBytes(conn.Do("GET", hash.Bytes()))
		if err != nil {
			content = nil
			log.Debugf(
				"content for %v not available in primary-, nor in template storage: %s",
				hash, err.Error())
		}

		return
	}()

	if content != nil {
		// store template content in primary storage asynchronously
		go func() {
			success, err := ds.setContent(hash, content)
			if err != nil {
				// we won't return error however, but just log it
				log.Infof("couldn't store template content in primary storage: %s", err.Error())
			} else if success {
				log.Debugf(
					"stored template content for %v in primary storage (asynchronously)",
					hash)
			}
		}()

		log.Debugf(
			"content not available in primary storage for %v, but did find it in template storage",
			hash)
	}

	// err = nil, content = ?
	return
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash zerodisk.Hash, content []byte) (success bool, err error) {
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

// Close implements backendStorage.Close
func (ds *dedupedStorage) Close() error { return nil }

// GoBackground implements backendStorage.GoBackground
func (ds *dedupedStorage) GoBackground(context.Context) {}
