package ardb

import (
	"context"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

func newSemiDedupedStorage(vdiskID string, blockSize int64, provider redisConnProvider, vlba *lba.LBA) backendStorage {
	storage := &semiDedupedStorage{
		templateStorage: newDedupedStorage(vdiskID, blockSize, provider, true, vlba),
		userStorage:     newNonDedupedStorage(vdiskID, "", blockSize, false, provider),
		vdiskID:         vdiskID,
		blockSize:       blockSize,
		provider:        provider,
	}

	err := storage.readBitMap()
	if err != nil {
		log.Debugf("couldn't read semi deduped storage %s's bitmap: %v", vdiskID, err)
	}

	return storage
}

// semiDedupedStorage is a backendStorage implementation,
// that stores the template content in the primary deduped storage,
// while it stores all user-written (and thus specific) data
// in the a nondeduped storage, both storages using the same storage servers.
type semiDedupedStorage struct {
	// used to store template data
	// (effectively read-only storage, from a user-perspective)
	templateStorage backendStorage
	// used to store user-specific data
	// e.g. Modified Registers, Applications, ...
	userStorage backendStorage

	// used to store the semi deduped metadata
	provider redisMetaConnProvider

	// bitmap used to indicate if the data is available as userdata or not
	userStorageBitMap bitMap

	// ID of this storage's vdisk
	vdiskID string

	// used when merging content
	blockSize int64
}

// Set implements backendStorage.Set
func (sds *semiDedupedStorage) Set(blockIndex int64, content []byte) error {
	err := sds.userStorage.Set(blockIndex, content)
	if err != nil {
		return err
	}

	// mark the bit in the bitmap,
	// such that the next time we retreive this block,
	// we know it has to be retreived from the user storage
	//
	// NOTE: for now this bit is never unset,
	// as it is assumed that once data is overwritten (if it's overwritten at all),
	// it is custom forever. It would be weird if suddenly out of the blue (after a delete operation for example),
	// the template (original) data would be used once again,
	// I don't think that's something a user would expect at all.
	sds.userStorageBitMap.Set(int(blockIndex))

	// delete content from dedup storage, as it's no longer needed there
	err = sds.templateStorage.Delete(blockIndex)
	if err != nil {
		// This won't be returned as an error,
		// as it's nothing critical,
		// it only means that deprecated data is not deleted.
		// It's not a critical error because the toggled bit in the bitmask,
		// will only make it look in the userstorage for this block anyhow.
		log.Error("semiDedupedStorage couldn't delete deprecated template data: ", err)
	}

	return nil
}

// Get implements backendStorage.Get
func (sds *semiDedupedStorage) Get(blockIndex int64) ([]byte, error) {
	// if a bit is enabled in the bitmap,
	// it means the data is stored in the user storage
	if sds.userStorageBitMap.Test(int(blockIndex)) {
		return sds.userStorage.Get(blockIndex)
	}

	return sds.templateStorage.Get(blockIndex)
}

// Delete implements backendStorage.Delete
func (sds *semiDedupedStorage) Delete(blockIndex int64) error {
	tErr := sds.templateStorage.Delete(blockIndex)

	// note that we don't unset the storage bit from the bitmask,
	// as that would basically flip it back to use dedup storage for this index,
	// which is not something we want,
	// as from a user perspective that already has been overwritten
	uErr := sds.userStorage.Delete(blockIndex)

	return combineErrorPair(tErr, uErr)
}

// Flush implements backendStorage.Flush
func (sds *semiDedupedStorage) Flush() error {
	tErr := sds.templateStorage.Flush()
	uErr := sds.userStorage.Flush()

	// serialize bitmap
	storageErr := combineErrorPair(tErr, uErr)
	bitmapErr := sds.writeBitMap()

	return combineErrorPair(storageErr, bitmapErr)
}

// Close implements backendStorage.Close
func (sds *semiDedupedStorage) Close() error {
	tErr := sds.templateStorage.Close()
	uErr := sds.userStorage.Close()
	return combineErrorPair(tErr, uErr)
}

// GoBackground implements backendStorage.GoBackground
func (sds *semiDedupedStorage) GoBackground(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	// start template-data background goroutine
	go func() {
		defer wg.Done()
		sds.templateStorage.GoBackground(ctx)
	}()

	// start user-data background goroutine
	go func() {
		defer wg.Done()
		sds.userStorage.GoBackground(ctx)
	}()

	wait := make(chan struct{})
	go func() {
		wg.Wait()
		close(wait)
	}()

	// wait for both background threads to be done
	// OR until shared context is done
	select {
	case <-wait:
		return

	case <-ctx.Done():
		return
	}
}

// readBitMap reads and decompresses (gzip) the bitmap from the ardb
func (sds *semiDedupedStorage) readBitMap() error {
	conn, err := sds.provider.MetaRedisConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	bytes, err := redis.Bytes(conn.Do("GET", SemiDedupBitMapKey(sds.vdiskID)))
	if err != nil {
		return err
	}

	return sds.userStorageBitMap.SetBytes(bytes)
}

// writeBitMap compresses and writes (gzip) the bitmap to the ardb
func (sds *semiDedupedStorage) writeBitMap() error {
	bytes, err := sds.userStorageBitMap.Bytes()
	if err != nil {
		return err
	}

	conn, err := sds.provider.MetaRedisConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", SemiDedupBitMapKey(sds.vdiskID), bytes)
	return err
}

func combineErrorPair(e1, e2 error) error {
	if e1 == nil {
		return e2
	}

	if e2 == nil {
		return e1
	}

	return fmt.Errorf("%v; %v", e1, e2)
}

// SemiDedupBitMapKey returns the storage key which is used
// to store the BitMap for the semideduped storage of a given vdisk
func SemiDedupBitMapKey(vdiskID string) string {
	return SemiDedupBitMapKeyPrefix + vdiskID
}

const (
	// SemiDedupBitMapKeyPrefix is the prefix used in SemiDedupBitMapKey
	SemiDedupBitMapKeyPrefix = "semidedup:bitmap:"
)
