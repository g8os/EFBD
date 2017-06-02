package ardb

import (
	"testing"

	"github.com/zero-os/0-Disk/redisstub"
)

func createTestNondedupedStorage(t *testing.T, vdiskID string, blockSize int64, provider *testRedisProvider) *nonDedupedStorage {
	return newNonDedupedStorage(vdiskID, blockSize, provider).(*nonDedupedStorage)
}

func TestNondedupedContent(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, 8, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorage(t, storage)
}

func TestNondedupedContentForceFlush(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, 8, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageForceFlush(t, storage)
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestNonDedupedDeadlock(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, blockSize, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageDeadlock(t, blockSize, blockCount, storage)
}
