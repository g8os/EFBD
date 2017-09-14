package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
		vdiskID, blockSize,
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
	tlogStorage, cleanup := func() (storage.BlockStorage, func()) {
		storage := storage.NewInMemoryStorage(vdiskID, blockSize)
		require.NotNil(t, storage)

		tlogrpc, cleanup := newTlogTestServer(context.Background(), t, vdiskID)
		require.NotEmpty(t, tlogrpc)

		source := config.NewStubSource()
		defer source.Close()
		source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
			Servers: []string{tlogrpc},
		})

		tls, err := newTlogStorage(ctx, vdiskID, "tlogcluster", source, blockSize, storage, provider, nil)
		require.NoError(t, err)
		require.NotNil(t, tls)

		return tls, cleanup
	}()
	defer cleanup()

	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, tlogStorage)
}

func testBackendSigtermHandler(ctx context.Context, t *testing.T, vdiskID string, blockSize int64, size uint64, storage storage.BlockStorage) {
	require.NotNil(t, storage)

	vComp := newVdiskCompletion()
	backend := newBackend(vdiskID, size, blockSize, storage, vComp, nil, dummyVdiskLogger{})
	require.NotNil(t, backend)

	go backend.GoBackground(ctx)
	defer backend.Close(ctx)

	someContent := make([]byte, blockSize)
	for i := range someContent {
		someContent[i] = byte(i % 255)
	}

	bw, err := backend.WriteAt(ctx, someContent, 0)
	require.NoError(t, err)
	require.Equal(t, blockSize, bw)

	bw, err = backend.WriteAt(ctx, someContent, blockSize)
	require.NoError(t, err)
	require.Equal(t, blockSize, bw)

	vComp.StopAll()

	// make sure we get completion error object
	errs := vComp.Wait()
	require.Equal(t, 0, len(errs), "expected no errs, but received: %v", errs)
}
