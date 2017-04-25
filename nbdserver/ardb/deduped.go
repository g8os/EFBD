package ardb

import (
	"context"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/nbdserver/lba"
)

// newDedupedStorage returns the deduped backendStorage implementation
func newDedupedStorage(vdiskID string, blockSize int64, provider redisConnectionProvider, vlba *lba.LBA) backendStorage {
	storage := &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: blockstor.HashBytes(make([]byte, blockSize)),
		provider:        provider,
		lba:             vlba,
		cancelChan:      make(chan struct{}, 1),
	}

	for i := 0; i < dedupBackgroundWorkerCount; i++ {
		storage.rcActionChannels[i] = make(chan rcAction, dedupBackgroundWorkerBufferSize)
	}

	return storage
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

	// used for background thread
	cancelChan       chan struct{}
	rcActionChannels [dedupBackgroundWorkerCount]chan rcAction
}

// Set implements backendStorage.Set
func (ds *dedupedStorage) Set(blockIndex int64, content []byte) (err error) {
	hash := blockstor.HashBytes(content)
	if ds.zeroContentHash.Equals(hash) {
		err = ds.lba.Delete(blockIndex)
		return
	}

	// Get Previous Hash
	prevHash, _ := ds.lba.Get(blockIndex)

	// reference the content to this vdisk,
	// and set the content itself, if it didn't exist yet
	err = ds.setContent(prevHash, hash, content)
	if err != nil {
		return
	}

	return ds.lba.Set(blockIndex, hash)
}

// Merge implements backendStorage.Merge
func (ds *dedupedStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	hash, _ := ds.lba.Get(blockIndex)

	var mergedContent []byte

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

	// delete the actual hash from the LBA first
	err = ds.lba.Delete(blockIndex)
	if err != nil {
		return
	}

	// only if the hash was succesfully deleted from the vdisk's LBA,
	// we should dereference the content,
	// and delete the content if the new reference count < 1
	ds.dereferenceContent(hash)
	return
}

// Flush implements backendStorage.Flush
func (ds *dedupedStorage) Flush() (err error) {
	err = ds.lba.Flush()
	return
}

// Close implements backendStorage.Close
func (ds *dedupedStorage) Close() error {
	ds.cancelChan <- struct{}{}
	return nil
}

// GoBackground implements backendStorage.GoBackground
func (ds *dedupedStorage) GoBackground(ctx context.Context) {
	var wg sync.WaitGroup

	for i := range ds.rcActionChannels {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			ds.goBackgroundWorker(ctx, workerID)
		}()
	}

	wg.Wait()
}

func (ds *dedupedStorage) goBackgroundWorker(ctx context.Context, workerID int) {
	log.Debugf("starting deduped background worker #%d", workerID+1)
	ch := ds.rcActionChannels[workerID]
	for {
		select {
		case action := <-ch:
			var err error

			switch action.Type {
			case rcIncrease:
				err = ds.goReferenceContent(action.Hash)
			case rcDecrease:
				err = ds.goDereferenceContent(action.Hash)
			default:
				err = fmt.Errorf(
					"background rc action %d for %v not recognized",
					action.Type, action.Hash)
			}

			if err != nil {
				log.Infof(
					"worker #%d: error during content (de)referencing: %s",
					workerID+1, err.Error())
			}

		case <-ds.cancelChan:
			log.Debugf("close dedupedStorage %q's background worker #%d",
				ds.vdiskID, workerID+1)
			return

		case <-ctx.Done():
			log.Debugf("forcefully exit dedupedStorage %q's background worker #%d",
				ds.vdiskID, workerID+1)
			return
		}
	}
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
			log.Debugf(
				"storing remote content for %v in local storage (asynchronously)",
				hash)
			conn, err := ds.getRedisConnection(hash)
			if err != nil {
				return
			}

			err = redisSendNow(conn, "SET", hash.Bytes(), content)
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

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(prevHash, curHash blockstor.Hash, content []byte) (err error) {
	err = func() (err error) {
		conn, err := ds.getRedisConnection(curHash)
		if err != nil {
			return
		}
		defer conn.Close()

		exists, err := redis.Bool(conn.Do("EXISTS", curHash.Bytes()))
		if err == nil && !exists {
			err = redisSendNow(conn, "SET", curHash.Bytes(), content)
		}

		return
	}()
	if err != nil {
		return
	}

	// reference currentHash
	ds.referenceContent(curHash)
	if prevHash != nil {
		// dereference previousHash
		ds.dereferenceContent(prevHash)
	}

	return nil
}

// sender of the reference content action
func (ds *dedupedStorage) referenceContent(hash blockstor.Hash) {
	workerID := dsWorkerID(hash)
	ds.rcActionChannels[workerID] <- rcAction{
		Type: rcIncrease,
		Hash: hash,
	}
}

// receiver of the reference content action
func (ds *dedupedStorage) goReferenceContent(hash blockstor.Hash) (err error) {
	key := dsReferenceKey(hash)

	conn, err := ds.getRedisConnection(hash)
	if err != nil {
		return
	}
	defer conn.Close()

	err = redisSendNow(conn, "INCR", key)
	return
}

// sender of the dereference content action
func (ds *dedupedStorage) dereferenceContent(hash blockstor.Hash) {
	workerID := dsWorkerID(hash)
	ds.rcActionChannels[workerID] <- rcAction{
		Type: rcDecrease,
		Hash: hash,
	}
}

// receiver of the dereference content action
func (ds *dedupedStorage) goDereferenceContent(hash blockstor.Hash) (err error) {
	key := dsReferenceKey(hash)

	conn, err := ds.getRedisConnection(hash)
	if err != nil {
		return
	}
	defer conn.Close()

	count, err := redis.Int64(conn.Do("DECR", key))
	if err != nil || count > 0 {
		return
	}

	err = redisSendNow(conn, "DEL", hash.Bytes(), key)
	return
}

func dsWorkerID(hash blockstor.Hash) int {
	return int(hash[0]) % dedupBackgroundWorkerCount
}

func dsReferenceKey(hash blockstor.Hash) (key []byte) {
	key = make([]byte, rcKeyPrefixLength+blockstor.HashSize)
	copy(key, rcKeyPrefix[:])
	copy(key[rcKeyPrefixLength:], hash[:])
	return
}

const (
	rcKeyPrefix       = "rc:"
	rcKeyPrefixLength = len(rcKeyPrefix)
)

const (
	dedupBackgroundWorkerCount      = 8
	dedupBackgroundWorkerBufferSize = 32
)

type rcAction struct {
	// Type of action
	Type rcActionType
	// Hash the actions applies on
	Hash blockstor.Hash
}

type rcActionType int

const (
	rcIncrease rcActionType = iota
	rcDecrease
)
