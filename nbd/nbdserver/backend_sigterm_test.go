package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/nbdserver/tlog"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestBackendSigtermHandler(t *testing.T) {
	cluster := redisstub.NewUniCluster(true)
	defer cluster.Close()

	const (
		vdiskID    = "a"
		size       = 64
		blockSize  = 8
		blockCount = size / blockSize
	)

	var blockStorage storage.BlockStorage
	ctx := context.Background()

	blockStorage, err := storage.Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, cluster, nil)
	if err != nil {
		t.Fatalf("couldn't create deduped block storage: %v", err)
	}

	// test Deduped Storage
	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, blockStorage)

	blockStorage, err = storage.NonDeduped(vdiskID, "", blockSize, cluster, nil)
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
		source.SetPrimaryStorageCluster(vdiskID, "nbdCluster", nil)
		source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
			Servers: []string{tlogrpc},
		})

		tls, err := tlog.Storage(ctx, vdiskID, source, blockSize, storage, cluster, nil)
		require.NoError(t, err)
		require.NotNil(t, tls)

		return tls, cleanup
	}()
	defer cleanup()

	testBackendSigtermHandler(ctx, t, vdiskID, blockSize, size, tlogStorage)
}

func newTlogTestServer(ctx context.Context, t *testing.T, vdiskID string) (string, func()) {
	testConf := &server.Config{
		DataShards:   4,
		ParityShards: 2,
		ListenAddr:   "",
		FlushSize:    25,
		FlushTime:    25,
		PrivKey:      "12345678901234567890123456789012",
	}

	configSource, _, cleanup := newZeroStorConfig(t, vdiskID, testConf)

	// start the server
	s, err := server.NewServer(testConf, configSource)
	require.Nil(t, err)

	go s.Listen(ctx)

	return s.ListenAddr(), cleanup
}

func newZeroStorConfig(t *testing.T, vdiskID string, tlogConf *server.Config) (*config.StubSource, stor.Config, func()) {

	// stor server
	storCluster, err := embeddedserver.NewZeroStorCluster(tlogConf.DataShards + tlogConf.ParityShards)
	require.Nil(t, err)

	var servers []config.ServerConfig
	for _, addr := range storCluster.Addrs() {
		servers = append(servers, config.ServerConfig{
			Address: addr,
		})
	}

	// meta server
	mdServer, err := embedserver.New()
	require.Nil(t, err)

	storConf := stor.Config{
		VdiskID:         vdiskID,
		Organization:    "testorg",
		Namespace:       "thedisk",
		ZeroStorShards:  storCluster.Addrs(),
		MetaShards:      []string{mdServer.ListenAddr()},
		DataShardsNum:   tlogConf.DataShards,
		ParityShardsNum: tlogConf.ParityShards,
		EncryptPrivKey:  tlogConf.PrivKey,
	}

	clusterID := "zero_stor_cluster_id"
	stubSource := config.NewStubSource()

	stubSource.SetTlogZeroStorCluster(vdiskID, clusterID, &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       storConf.Organization,
			Namespace: storConf.Namespace,
			ClientID:  storConf.IyoClientID,
			Secret:    storConf.IyoSecret,
		},
		Servers: servers,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	})

	cleanFunc := func() {
		mdServer.Stop()
		storCluster.Close()
	}
	return stubSource, storConf, cleanFunc
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
