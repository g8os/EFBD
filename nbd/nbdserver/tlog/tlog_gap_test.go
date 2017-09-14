package tlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
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
		storage:      storage.NewInMemoryStorage(vdiskID, blockSize),
		modSleepTime: sleepTime,
	}
	if !assert.NotNil(t, slowStorage) || !assert.NotNil(t, slowStorage.storage) {
		return
	}

	tlogrpc, cleanup := newTlogTestServer(ctx, t, vdiskID)
	defer cleanup()
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	source := config.NewStubSource()
	source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
		Servers: []string{tlogrpc},
	})
	defer source.Close()

	storage, err := Storage(
		ctx, vdiskID, "tlogcluster", source, blockSize, slowStorage, nil, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}
	defer storage.Close()

	var wg sync.WaitGroup
	for i := int64(0); i < blockCount; i++ {
		wg.Add(1)

		go func(blockIndex int64) {
			defer wg.Done()

			preContent := make([]byte, blockSize)
			rand.Read(preContent)

			// set content
			if err := storage.SetBlock(blockIndex, preContent); err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			time.Sleep(sleepTime)

			// get content
			postContent, err := storage.GetBlock(blockIndex)
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
	storage      storage.BlockStorage
	modSleepTime time.Duration
}

// SetBlock implements BlockStorage.SetBlock
func (ms *slowInMemoryStorage) SetBlock(blockIndex int64, content []byte) error {
	time.Sleep(ms.modSleepTime)
	return ms.storage.SetBlock(blockIndex, content)
}

// GetBlock implements BlockStorage.GetBlock
func (ms *slowInMemoryStorage) GetBlock(blockIndex int64) ([]byte, error) {
	return ms.storage.GetBlock(blockIndex)
}

// DeleteBlock implements BlockStorage.DeleteBlock
func (ms *slowInMemoryStorage) DeleteBlock(blockIndex int64) error {
	time.Sleep(ms.modSleepTime)
	return ms.storage.DeleteBlock(blockIndex)
}

// Flush implements BlockStorage.Flush
func (ms *slowInMemoryStorage) Flush() error {
	return ms.storage.Flush()
}

// Close implements BlockStorage.Close
func (ms *slowInMemoryStorage) Close() error {
	return ms.storage.Close()
}
