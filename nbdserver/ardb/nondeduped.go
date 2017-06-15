package ardb

import (
	"context"

	"github.com/zero-os/0-Disk/log"
)

// newNonDedupedStorage returns the non deduped backendStorage implementation
func newNonDedupedStorage(vdiskID, rootVdiskID string, blockSize int64, templateSupport bool, provider redisConnectionProvider) backendStorage {
	nondeduped := &nonDedupedStorage{
		blockSize:   blockSize,
		vdiskID:     vdiskID,
		rootVdiskID: rootVdiskID,
		provider:    provider,
	}

	if templateSupport {
		nondeduped.getContent = nondeduped.getLocalOrRemoteContent
	} else {
		nondeduped.getContent = nondeduped.getLocalContent
	}

	return nondeduped
}

// nonDedupedStorage is a backendStorage implementation,
// that simply stores each block in redis using
// a unique key based on the vdiskID and blockIndex
type nonDedupedStorage struct {
	blockSize   int64
	vdiskID     string
	rootVdiskID string
	provider    redisConnectionProvider
	getContent  nondedupedContentGetter
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type nondedupedContentGetter func(blockIndex int64) (content []byte, err error)

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
	mergedContent, _ := ss.getContent(blockIndex)

	if ocl := int64(len(mergedContent)); ocl == 0 {
		mergedContent = make([]byte, ss.blockSize)
	} else if ocl < ss.blockSize {
		oc := make([]byte, ss.blockSize)
		copy(oc, mergedContent)
		mergedContent = oc
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// store new content, as the merged version is non-zero
	_, err = conn.Do("HSET", ss.vdiskID, blockIndex, mergedContent)
	return
}

// Get implements backendStorage.Get
func (ss *nonDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	content, err = ss.getContent(blockIndex)
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

func (ss *nonDedupedStorage) getLocalContent(blockIndex int64) (content []byte, err error) {
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	content, err = redisBytes(conn.Do("HGET", ss.vdiskID, blockIndex))
	return
}

func (ss *nonDedupedStorage) getLocalOrRemoteContent(blockIndex int64) (content []byte, err error) {
	content, err = ss.getLocalContent(blockIndex)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	content = func() (content []byte) {
		conn, err := ss.provider.FallbackRedisConnection(blockIndex)
		if err != nil {
			log.Debugf(
				"no local content available for block %d and no remote storage available: %s",
				blockIndex, err.Error())
			return
		}
		defer conn.Close()

		content, err = redisBytes(conn.Do("HGET", ss.rootVdiskID, blockIndex))
		if err != nil {
			log.Debugf(
				"content for block %d (vdisk %s) not available in local-, nor in remote storage: %s",
				blockIndex, ss.rootVdiskID, err.Error())
			content = nil
		}

		return
	}()

	if content != nil {
		// store remote content in local storage asynchronously
		go func() {
			err := ss.Set(blockIndex, content)
			if err != nil {
				// we won't return error however, but just log it
				log.Infof(
					"couldn't store remote content block %d in local storage: %s",
					blockIndex, err.Error())
			}
		}()

		log.Debugf(
			"no local content block %d available, but did find remotely",
			blockIndex)
	}

	return
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
