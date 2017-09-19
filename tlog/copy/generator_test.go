package copy

import (
	"context"
	"crypto/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestGenerate(t *testing.T) {
	const (
		sourceVdiskID     = "sourceID"
		targetVdiskID     = "targetID"
		dataShards        = 4
		parityShards      = 2
		size              = 64
		blockSize         = 4096
		blockCount        = 1000
		privKey           = "12345678901234567890123456789012"
		zeroStorClusterID = "zero_stor_cluster_id"
		nbdClusterID      = "nbd_cluster_id"
	)

	// 0-stor servers
	storCluster, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.NoError(t, err)
	defer storCluster.Close()

	mdServer, err := embedserver.New()
	require.NoError(t, err)
	defer mdServer.Stop()

	// redis provider
	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	defer redisProvider.Close()

	// config source
	confSource := config.NewStubSource()
	defer confSource.Close()

	staticConf := config.VdiskStaticConfig{
		BlockSize: blockSize,
		Size:      2,
		Type:      config.VdiskTypeBoot,
	}

	var serverConf []config.ServerConfig
	for _, addr := range storCluster.Addrs() {
		serverConf = append(serverConf, config.ServerConfig{
			Address: addr,
		})
	}

	confSource.SetVdiskConfig(sourceVdiskID, &staticConf)
	confSource.SetVdiskConfig(targetVdiskID, &staticConf)

	storageClusterConf := &config.StorageClusterConfig{
		DataStorage: []config.StorageServerConfig{
			config.StorageServerConfig{
				Address: redisProvider.PrimaryAddress(),
			},
		},
	}

	confSource.SetPrimaryStorageCluster(sourceVdiskID, nbdClusterID, storageClusterConf)
	confSource.SetPrimaryStorageCluster(targetVdiskID, nbdClusterID, storageClusterConf)

	confSource.SetTlogZeroStorCluster(targetVdiskID, zeroStorClusterID, &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       "testorg",
			Namespace: "thedisk",
		},
		Servers: serverConf,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	})

	// 1. Create block storages and fill with data
	sourceBlockStorage, err := storage.Deduped(
		sourceVdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	require.NoError(t, err)

	contents := make(map[int64][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		// generates content
		content := make([]byte, blockSize)
		_, err := rand.Read(content)
		require.NoError(t, err)
		contents[int64(i)] = content
	}

	for idx, content := range contents {
		err := sourceBlockStorage.SetBlock(idx, content)
		require.NoError(t, err)
	}
	err = sourceBlockStorage.Flush()
	require.NoError(t, err)

	// 2. Generate tlog data
	generator, err := newGenerator(confSource, Config{
		SourceVdiskID: sourceVdiskID,
		TargetVdiskID: targetVdiskID,
		DataShards:    dataShards,
		ParityShards:  parityShards,
		PrivKey:       privKey,
		JobCount:      runtime.NumCPU(),
	})
	require.NoError(t, err)

	err = generator.GenerateFromStorage(context.Background())
	require.NoError(t, err)

	// 3. Use tlog replay to restore data
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	player, err := player.NewPlayer(ctx, confSource, targetVdiskID, privKey, dataShards, parityShards)
	require.NoError(t, err)

	_, err = player.Replay(decoder.NewLimitByTimestamp(0, 0))
	require.NoError(t, err)

	// 4. Check the replayed data
	targetBlockStorage, err := storage.Deduped(
		targetVdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	require.NoError(t, err)

	for idx, content := range contents {
		block, err := targetBlockStorage.GetBlock(idx)
		require.NoError(t, err)

		require.Equal(t, block, content)
	}
}
