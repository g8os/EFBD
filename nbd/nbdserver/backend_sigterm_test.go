package main

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestBackendSigtermHandler(t *testing.T) {
	provider := redisstub.NewInMemoryRedisProvider(nil)

	const (
		vdiskID    = "a"
		size       = 64
		blockSize  = 8
		blockCount = size / blockSize
	)

	var err error
	var blockStorage storage.BlockStorage
	ctx := context.Background()

	blockStorage, err = storage.Deduped(
		vdiskID, blockCount*blockSize, blockSize,
		ardb.DefaultLBACacheLimit, false, provider)
	if err != nil {
		t.Fatalf("couldn't create deduped block storage: %v", err)
	}

	// test Deduped Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, blockStorage)

	blockStorage, err = storage.NonDeduped(vdiskID, "", blockSize, false, provider)
	if err != nil {
		t.Fatalf("couldn't create nondeduped block storage: %v", err)
	}

	// test Non Deduped Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, blockStorage)

	// test Tlog Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, func() storage.BlockStorage {
		storage := newInMemoryStorage(vdiskID, blockSize)
		if !assert.NotNil(t, storage) {
			return nil
		}

		tlogrpc := newTlogTestServer(context.Background(), t)
		if !assert.NotEmpty(t, tlogrpc) {
			return nil
		}

		source := config.NewStubSource()
		defer source.Close()
		source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
			Servers: []string{tlogrpc},
		})

		// TODO: set addresses into source...

		tls, err := newTlogStorage(ctx, vdiskID, "tlogcluster", source, blockSize, storage, nil)
		if !assert.NoError(t, err) || !assert.NotNil(t, tls) {
			return nil
		}

		return tls
	}())
}

func testBackendSigtermHandler(ctx context.Context, t *testing.T, vdiskID string, blockSize int64, size uint64, storage storage.BlockStorage) {
	if !assert.NotNil(t, storage) {
		return
	}

	vComp := new(vdiskCompletion)
	backend := newBackend(vdiskID, size, blockSize, storage, vComp, nil)
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
