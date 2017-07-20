package ardb

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestBackendSigtermHandler(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	provider := newTestRedisProvider(memRedis, nil) // template = nil

	const (
		vdiskID    = "a"
		size       = 64
		blockSize  = 8
		blockCount = size / blockSize
	)

	ctx := context.Background()

	// test Deduped Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size,
		createTestDedupedStorage(t, vdiskID, blockSize, blockCount, false, provider))

	// test Non Deduped Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size,
		createTestNondedupedStorage(t, vdiskID, blockSize, false, provider))

	// test Tlog Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, func() backendStorage {
		storage := newInMemoryStorage(vdiskID, blockSize)
		if !assert.NotNil(t, storage) {
			return nil
		}

		tlogrpc := newTlogTestServer(context.Background(), t)
		if !assert.NotEmpty(t, tlogrpc) {
			return nil
		}

		tls, err := newTlogStorage(vdiskID, tlogrpc, "", blockSize, storage)
		if !assert.NoError(t, err) || !assert.NotNil(t, tls) {
			return nil
		}

		return tls
	}())
}

func testBackendSigtermHandler(ctx context.Context, t *testing.T, vdiskID string, blockSize int64, size uint64, storage backendStorage) {
	if !assert.NotNil(t, storage) {
		return
	}

	vComp := new(vdiskCompletion)
	backend := newBackend(vdiskID, blockSize, size, storage, nil, vComp)
	if !assert.NotNil(t, backend) {
		return
	}
	go backend.GoBackground(ctx)
	defer backend.Close(ctx)

	someContent := make([]byte, blockSize)
	for i := range someContent {
		someContent[i] = byte(i % 255)
	}

	bw, err := backend.WriteAt(ctx, someContent, 0)
	if !assert.NoError(t, err) || !assert.Equal(t, blockSize, bw) {
		return
	}
	bw, err = backend.WriteAt(ctx, someContent, blockSize)
	if !assert.NoError(t, err) || !assert.Equal(t, blockSize, bw) {
		return
	}

	// sending SIGTERM
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	// make sure we get completion error object
	errs := vComp.Wait()
	assert.Equal(t, 0, len(errs), "expected no errs, but received: %v", errs)
}
