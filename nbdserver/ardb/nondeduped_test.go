package ardb

import (
	"context"
	"testing"

	"github.com/g8os/blockstor/redisstub"
)

func createTestNondedupedStorage(t *testing.T, vdiskID string, blockSize int64, provider *testRedisProvider) *nonDedupedStorage {
	return newNonDedupedStorage(vdiskID, blockSize, provider).(*nonDedupedStorage)
}

func BenchmarkTimeOfKeyCreation(b *testing.B) {
	s := &nonDedupedStorage{
		vdiskID: "MyVdiskID",
	}
	for i := 0; i < b.N; i++ {
		s.getKey(int64(i))
	}

}

func TestNondedupedContent(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	var (
		ctx = context.Background()
	)

	redisProvider := &testRedisProvider{memRedis, nil} // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, 8, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}
	defer storage.Close()
	go storage.GoBackground(ctx)

	testBackendStorage(t, storage)
}
