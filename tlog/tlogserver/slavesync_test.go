package main

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-Disk/tlog/tlogserver/slavesync"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestSlaveSyncEndToEnd(t *testing.T) {
	const (
		vdiskID   = "1234567890"
		blockSize = 4096
	)
	conf := server.DefaultConfig()
	conf.ListenAddr = ""

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// redis provider for the ardb
	mr := redisstub.NewMemoryRedis()
	defer mr.Close()

	// 0-stor
	stubSource, _, cleanFunc := newZeroStorConfig(t, vdiskID, conf.PrivKey, 1, 1, mr.StorageServerConfig().Address)
	defer cleanFunc()

	// Slave syncer
	ssm := slavesync.NewManager(ctx, stubSource, conf.PrivKey)
	conf.SlaveSyncerMgr = ssm

	// TLOG

	s, err := server.NewServer(conf, stubSource)
	require.Nil(t, err)

	go s.Listen(ctx)

	// create tlog client
	client, err := tlogclient.New([]string{s.ListenAddr()}, vdiskID)
	require.Nil(t, err)

	// initialize test data
	numLogs := conf.FlushSize - 5 // number of logs to send.
	data := make([]byte, blockSize*numLogs)
	rand.Read(data)

	// send tlog
	for i := 0; i < numLogs; i++ {
		offset := i * blockSize

		x := uint64(i)

		// check we can send it without error
		err := client.Send(schema.OpSet, x+1, int64(x), int64(x), data[offset:offset+blockSize])
		require.Nil(t, err)
	}
	maxSeq := uint64(numLogs)

	// flush the logs
	err = client.ForceFlushAtSeq(maxSeq)
	require.Nil(t, err)

	flushedCh := make(chan struct{}, 1)
	// start the client receiver to wait for the flush
	go func() {
		for res := range client.Recv() {
			require.Nil(t, res.Err)
			if res.Resp.Status == tlog.BlockStatusFlushOK {
				if res.Resp.Sequences[len(res.Resp.Sequences)-1] == maxSeq {
					flushedCh <- struct{}{}
				}
			}
		}
	}()

	<-flushedCh

	// sync to slave
	err = client.WaitNbdSlaveSync()
	require.Nil(t, err)

	pool := ardb.NewPool(nil)
	defer pool.Close()

	cluster, err := ardb.NewUniCluster(mr.StorageServerConfig(), pool)
	require.NoError(t, err)

	// check it from the storage
	storage, err := storage.Deduped(storage.BlockStorageConfig{VdiskID: vdiskID, BlockSize: blockSize, LBACacheLimit: ardb.DefaultLBACacheLimit, BufferSize: 10},
		cluster, nil)
	require.NoError(t, err)

	for i := 0; i < numLogs; i++ {
		offset := i * blockSize

		content, err := storage.GetBlock(int64(i))
		require.NoError(t, err)

		require.Equal(t, data[offset:offset+blockSize], content, "Invalid block index = %v", i)
	}
}

func newZeroStorConfig(t *testing.T, vdiskID, privKey string,
	data, parity int, ardbAddress string) (*config.StubSource, stor.Config, func()) {
	// stor server
	storCluster, err := embeddedserver.NewZeroStorCluster(data + parity)
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
		Organization:    "tesorg",
		Namespace:       "thedisk",
		IyoClientID:     "",
		IyoSecret:       "",
		ZeroStorShards:  storCluster.Addrs(),
		MetaShards:      []string{mdServer.ListenAddr()},
		DataShardsNum:   data,
		ParityShardsNum: parity,
		EncryptPrivKey:  privKey,
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
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
		DataServers:  servers,
		DataShards:   data,
		ParityShards: parity,
	})

	storageClusterID := "cluster_id_for_slave"
	storageCluserConf := &config.StorageClusterConfig{
		Servers: []config.StorageServerConfig{
			config.StorageServerConfig{
				Address: ardbAddress,
			}},
	}

	stubSource.SetSlaveStorageCluster(vdiskID, storageClusterID, storageCluserConf)
	stubSource.SetPrimaryStorageCluster(vdiskID, storageClusterID, storageCluserConf)
	cleanFunc := func() {
		mdServer.Stop()
		storCluster.Close()
	}
	return stubSource, storConf, cleanFunc
}

func init() {
	log.SetLevel(log.DebugLevel)
}
