package ardb

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// to easily reproduce and test:
// https://github.com/zero-os/0-Disk/issues/231
func TestTlogStorageSlow(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const sleepTime = time.Millisecond * 25

	slowStorage := &slowInMemoryStorage{
		storage:      newInMemoryStorage(vdiskID, blockSize).(*inMemoryStorage),
		modSleepTime: sleepTime,
	}
	if !assert.NotNil(t, slowStorage) || !assert.NotNil(t, slowStorage.storage) {
		return
	}

	tlogrpc := newTlogTestServer(ctx, t)
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	storage, err := newTlogStorage(vdiskID, tlogrpc, "", blockSize, slowStorage)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}
	go storage.GoBackground(ctx)
	defer storage.Close()

	var wg sync.WaitGroup
	for i := int64(0); i < blockCount; i++ {
		wg.Add(1)

		go func(blockIndex int64) {
			defer wg.Done()

			preContent := make([]byte, blockSize)
			rand.Read(preContent)

			// set content
			if err := storage.Set(blockIndex, preContent); err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			time.Sleep(sleepTime)

			// get content
			postContent, err := storage.Get(blockIndex)
			if err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			// make sure the postContent (GET) equals the preContent (SET)
			if bytes.Compare(preContent, postContent) != 0 {
				t.Fatal(blockIndex, "unexpected content received", preContent, postContent)
			}
		}(i)
	}

	wg.Wait()
}

// slowInMemoryStorage is a wrapper of the inMemoryStorage
// which simply slows done the speed of its operations
// by sleeping before actually running a modification command.
// Used only in TestTlogStorageSlow.
type slowInMemoryStorage struct {
	storage      *inMemoryStorage
	modSleepTime time.Duration
}

// Set implements backendStorage.Set
func (ms *slowInMemoryStorage) Set(blockIndex int64, content []byte) error {
	time.Sleep(ms.modSleepTime)
	return ms.storage.Set(blockIndex, content)
}

// Get implements backendStorage.Get
func (ms *slowInMemoryStorage) Get(blockIndex int64) ([]byte, error) {
	return ms.storage.Get(blockIndex)
}

// Delete implements backendStorage.Delete
func (ms *slowInMemoryStorage) Delete(blockIndex int64) error {
	time.Sleep(ms.modSleepTime)
	return ms.storage.Delete(blockIndex)
}

// Flush implements backendStorage.Flush
func (ms *slowInMemoryStorage) Flush() error {
	return ms.storage.Flush()
}

// Close implements backendStorage.Close
func (ms *slowInMemoryStorage) Close() error {
	return ms.storage.Close()
}

// GoBackground implements backendStorage.GoBackground
func (ms *slowInMemoryStorage) GoBackground(ctx context.Context) {
	ms.storage.GoBackground(ctx)
}
