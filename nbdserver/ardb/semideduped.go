package ardb

import (
	"context"
	"fmt"
	"sync"

	"github.com/siddontang/go/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

func newSemiDedupedStorage(vdiskID string, blockSize int64, provider redisConnectionProvider, vlba *lba.LBA) backendStorage {
	return &semiDedupedStorage{
		templateStorage: newDedupedStorage(vdiskID, blockSize, provider, true, vlba),
		userStorage:     newNonDedupedStorage(vdiskID, "", blockSize, false, provider),
		blockSize:       blockSize,
	}
}

// semiDedupedStorage is a backendStorage implementation,
// that stores the template content in a local deduped storage,
// while it stores all user-written (and thus specific) data
// in the a nondeduped storage, both storages using the same storage servers.
type semiDedupedStorage struct {
	// used to store template data
	// (effectively read-only storage, from a user-perspective)
	templateStorage backendStorage
	// used to store user-specific data
	// e.g. Modified Registers, Applications, ...
	userStorage backendStorage

	blockSize int64
}

// Set implements backendStorage.Set
func (sds *semiDedupedStorage) Set(blockIndex int64, content []byte) error {
	return sds.userStorage.Set(blockIndex, content)
}

// Merge implements backendStorage.Merge
func (sds *semiDedupedStorage) Merge(blockIndex, offset int64, content []byte) error {
	mergedContent, _ := sds.Get(blockIndex)

	if ocl := int64(len(mergedContent)); ocl == 0 {
		mergedContent = make([]byte, sds.blockSize)
	} else if ocl < sds.blockSize {
		oc := make([]byte, sds.blockSize)
		copy(oc, mergedContent)
		mergedContent = oc
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content
	return sds.userStorage.Set(blockIndex, mergedContent)
}

// Get implements backendStorage.Get
func (sds *semiDedupedStorage) Get(blockIndex int64) (content []byte, err error) {
	// try to get the content as user-specific content,
	// which has priority, as it is assumed to be newer content
	content, err = sds.userStorage.Get(blockIndex)
	if err == nil && content != nil {
		return
	}
	if err != nil {
		log.Errorf(
			"semiDedupedStorage received error while gettng user-specific content: %s",
			err.Error())
	}

	// otherwise it has to be template-specific content,
	// if the block index is to be valid at all
	content, err = sds.templateStorage.Get(blockIndex)
	return
}

// Delete implements backendStorage.Delete
func (sds *semiDedupedStorage) Delete(blockIndex int64) error {
	tErr := sds.templateStorage.Delete(blockIndex)
	uErr := sds.userStorage.Delete(blockIndex)
	return combineErrorPair(tErr, uErr)
}

// Flush implements backendStorage.Flush
func (sds *semiDedupedStorage) Flush() error {
	tErr := sds.templateStorage.Flush()
	uErr := sds.userStorage.Flush()
	return combineErrorPair(tErr, uErr)
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

func combineErrorPair(e1, e2 error) error {
	if e1 == nil {
		return e2
	}

	if e2 == nil {
		return e1
	}

	return fmt.Errorf("%v; %v", e1, e2)
}
