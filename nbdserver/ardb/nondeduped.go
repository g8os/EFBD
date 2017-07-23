package ardb

import (
	"context"

	"github.com/zero-os/0-Disk/log"
)

// newNonDedupedStorage returns the non deduped backendStorage implementation
func newNonDedupedStorage(vdiskID, templateVdiskID string, blockSize int64, templateSupport bool, provider redisDataConnProvider) backendStorage {
	nondeduped := &nonDedupedStorage{
		blockSize:          blockSize,
		storageKey:         NonDedupedStorageKey(vdiskID),
		templateStorageKey: NonDedupedStorageKey(templateVdiskID),
		vdiskID:            vdiskID,
		templateVdiskID:    templateVdiskID,
		provider:           provider,
	}

	if templateSupport {
		nondeduped.getContent = nondeduped.getPrimaryOrTemplateContent
		if templateVdiskID == "" {
			nondeduped.templateVdiskID = vdiskID
		}
	} else {
		nondeduped.getContent = nondeduped.getPrimaryContent
	}

	return nondeduped
}

// nonDedupedStorage is a backendStorage implementation,
// which simply stores each block in redis using
// a unique key based on the vdiskID and blockIndex.
type nonDedupedStorage struct {
	blockSize          int64                   // blocksize in bytes
	storageKey         string                  // Storage Key based on vdiskID
	templateStorageKey string                  // Storage Key based on templateVdiskID
	vdiskID            string                  // ID for the vdisk
	templateVdiskID    string                  // used in case template is supposed (same value as vdiskID if not defined)
	provider           redisDataConnProvider   // used to get the connection info to storage servers
	getContent         nondedupedContentGetter // getter depends on whether there is template support or not
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type nondedupedContentGetter func(blockIndex int64) (content []byte, err error)

// Set implements backendStorage.Set
func (ss *nonDedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	// get a connection to a data storage server, based on the modulo blockIndex
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
		_, err = conn.Do("HDEL", ss.storageKey, blockIndex)
		return
	}

	// content is not zero, so let's (over)write it
	_, err = conn.Do("HSET", ss.storageKey, blockIndex, content)
	return
}

// Get implements backendStorage.Get
func (ss *nonDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	content, err = ss.getContent(blockIndex)
	return
}

// Delete implements backendStorage.Delete
func (ss *nonDedupedStorage) Delete(blockIndex int64) (err error) {
	// get a connection to a data storage server, based on the modulo blockIndex
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// delete the block defined for the block index (if it previously existed at all)
	_, err = conn.Do("HDEL", ss.storageKey, blockIndex)
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

// (*nonDedupedStorage).getContent in case storage has no template support
func (ss *nonDedupedStorage) getPrimaryContent(blockIndex int64) (content []byte, err error) {
	// get a connection to a data storage server, based on the modulo blockIndex
	conn, err := ss.provider.RedisConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// get block from primary data storage server, if it exists at all,
	// a nil block is returned in case it didn't exist
	content, err = redisBytes(conn.Do("HGET", ss.storageKey, blockIndex))
	return
}

// (*nonDedupedStorage).getContent in case storage has template support
func (ss *nonDedupedStorage) getPrimaryOrTemplateContent(blockIndex int64) (content []byte, err error) {
	content, err = ss.getPrimaryContent(blockIndex)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	content = func() (content []byte) {
		// get a connection to a template data storage server, based on the modulo blockIndex
		conn, err := ss.provider.TemplateRedisConnection(blockIndex)
		if err != nil {
			log.Debugf(
				"block %d not available in primary storage and no template storage available: %s",
				blockIndex, err.Error())
			return
		}
		defer conn.Close()

		// get block from template data storage server, if it exists at all,
		// a nil block is returned in case it didn't exist
		content, err = redisBytes(conn.Do("HGET", ss.templateStorageKey, blockIndex))
		if err != nil {
			log.Debugf(
				"content for block %d (vdisk %s) not available in primary-, nor in template storage: %s",
				blockIndex, ss.templateVdiskID, err.Error())
			content = nil
		}

		return
	}()

	// check if we found the content in the template server
	if content != nil {
		// store template content in primary storage asynchronously
		go func() {
			err := ss.Set(blockIndex, content)
			if err != nil {
				// we won't return error however, but just log it
				log.Infof(
					"couldn't store template content block %d in primary storage: %s",
					blockIndex, err.Error())
			}
		}()

		log.Debugf(
			"block %d not available in primary storage, but did find it in template storage",
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

// NonDedupedStorageKey returns the storage key that can/will be
// used to store the nondeduped data for the given vdiskID
func NonDedupedStorageKey(vdiskID string) string {
	return NonDedupedStorageKeyPrefix + vdiskID
}

const (
	// NonDedupedStorageKeyPrefix is the prefix used in NonDedupedStorageKey
	NonDedupedStorageKeyPrefix = "nondedup:"
)
