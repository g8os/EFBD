package ardb

import (
	"context"
	"sync"

	"github.com/zero-os/0-Disk/log"
)

// newInMemoryStorage returns the in-memory backendStorage implementation
func newInMemoryStorage(vdiskID string, blockSize int64) backendStorage {
	return &inMemoryStorage{
		blockSize: blockSize,
		vdiskID:   vdiskID,
		vdisk:     make(map[int64][]byte),
	}
}

// inMemoryStorage is a backendStorage implementation,
// that simply stores each block in-memory,
// only meant for dev and test purposes.
type inMemoryStorage struct {
	blockSize int64
	vdiskID   string
	vdisk     map[int64][]byte
	mux       sync.RWMutex
}

// Set implements backendStorage.Set
func (ms *inMemoryStorage) Set(blockIndex int64, content []byte) (err error) {
	ms.mux.Lock()
	defer ms.mux.Unlock()

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ms.isZeroContent(content) {
		log.Debugf(
			"deleting content @ %d for vdisk %s as it's an all zeroes block",
			blockIndex, ms.vdiskID)
		delete(ms.vdisk, blockIndex)
		return
	}

	// content is not zero, so let's (over)write it
	ms.vdisk[blockIndex] = content
	return
}

// Merge implements backendStorage.Merge
func (ms *inMemoryStorage) Merge(blockIndex, offset int64, content []byte) (err error) {
	ms.mux.Lock()
	defer ms.mux.Unlock()

	mergedContent, _ := ms.vdisk[blockIndex]
	if ocl := int64(len(mergedContent)); ocl == 0 {
		mergedContent = make([]byte, ms.blockSize)
	} else if ocl < ms.blockSize {
		oc := make([]byte, ms.blockSize)
		copy(oc, mergedContent)
		mergedContent = oc
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content, as the merged version is non-zero
	ms.vdisk[blockIndex] = mergedContent
	return
}

// Get implements backendStorage.Get
func (ms *inMemoryStorage) Get(blockIndex int64) (content []byte, err error) {
	ms.mux.RLock()
	defer ms.mux.RUnlock()

	content, _ = ms.vdisk[blockIndex]
	return
}

// Delete implements backendStorage.Delete
func (ms *inMemoryStorage) Delete(blockIndex int64) (err error) {
	ms.mux.Lock()
	defer ms.mux.Unlock()

	delete(ms.vdisk, blockIndex)
	return
}

// Flush implements backendStorage.Flush
func (ms *inMemoryStorage) Flush() (err error) {
	// nothing to do for the in-memory backendStorage
	return
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ms *inMemoryStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}

// Close implements backendStorage.Close
func (ms *inMemoryStorage) Close() error { return nil }

// GoBackground implements backendStorage.GoBackground
func (ms *inMemoryStorage) GoBackground(context.Context) {}
