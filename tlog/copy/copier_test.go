package copy

import (
	"crypto/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

// TestCopyDiffCluster test tlog copy when target & source vdisk reside in different tlog cluster
func TestCopyDiffCluster(t *testing.T) {
	const (
		sourceVdiskID           = "sourceID"
		targetVdiskID           = "targetID"
		dataShards              = 4
		parityShards            = 2
		size                    = 64
		blockSize               = 4096
		privKey                 = "12345678901234567890123456789012"
		zeroStorClusterIDSource = "zero_stor_cluster_id_source"
		zeroStorClusterIDTarget = "zero_stor_cluster_id_target"
	)

	// creates zero-stor cluster
	storClusterSource, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.NoError(t, err)
	defer storClusterSource.Close()

	storClusterTarget, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.NoError(t, err)
	defer storClusterTarget.Close()

	mdServer, err := embedserver.New()
	require.NoError(t, err)
	defer mdServer.Stop()

	// config source
	confSource := config.NewStubSource()
	defer confSource.Close()

	var serverConfSource []config.ServerConfig
	for _, addr := range storClusterSource.Addrs() {
		serverConfSource = append(serverConfSource, config.ServerConfig{
			Address: addr,
		})
	}

	var serverConfTarget []config.ServerConfig
	for _, addr := range storClusterTarget.Addrs() {
		serverConfTarget = append(serverConfTarget, config.ServerConfig{
			Address: addr,
		})
	}

	staticConf := config.VdiskStaticConfig{
		BlockSize: blockSize,
		Size:      2,
		Type:      config.VdiskTypeBoot,
	}

	confSource.SetVdiskConfig(sourceVdiskID, &staticConf)
	confSource.SetVdiskConfig(targetVdiskID, &staticConf)

	// configure tlog cluster
	zeroStorClusterConfSource := &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       "testorg",
			Namespace: "thedisk",
		},
		Servers: serverConfSource,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	}

	zeroStorClusterConfTarget := &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       "testorg",
			Namespace: "thedisk",
		},
		Servers: serverConfTarget,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	}

	confSource.SetTlogZeroStorCluster(sourceVdiskID, zeroStorClusterIDSource, zeroStorClusterConfSource)
	confSource.SetTlogZeroStorCluster(targetVdiskID, zeroStorClusterIDTarget, zeroStorClusterConfTarget)
	testCopy(t, confSource, dataShards, parityShards, blockSize, sourceVdiskID, targetVdiskID, privKey, true)
}

// TestCopySameCluster test tlog copy when both vdisks reside in same zerostor cluster
func TestCopySameCluster(t *testing.T) {
	const (
		sourceVdiskID     = "sourceID"
		targetVdiskID     = "targetID"
		dataShards        = 4
		parityShards      = 2
		size              = 64
		blockSize         = 4096
		privKey           = "12345678901234567890123456789012"
		zeroStorClusterID = "zero_stor_cluster_id"
	)

	// creates zero-stor cluster
	storCluster, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.NoError(t, err)
	defer storCluster.Close()

	mdServer, err := embedserver.New()
	require.NoError(t, err)
	defer mdServer.Stop()

	// config source
	confSource := config.NewStubSource()
	defer confSource.Close()

	var serverConf []config.ServerConfig
	for _, addr := range storCluster.Addrs() {
		serverConf = append(serverConf, config.ServerConfig{
			Address: addr,
		})
	}

	staticConf := config.VdiskStaticConfig{
		BlockSize: blockSize,
		Size:      2,
		Type:      config.VdiskTypeBoot,
	}

	confSource.SetVdiskConfig(sourceVdiskID, &staticConf)
	confSource.SetVdiskConfig(targetVdiskID, &staticConf)

	// configure tlog cluster
	zeroStorClusterConf := &config.ZeroStorClusterConfig{
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
	}

	confSource.SetTlogZeroStorCluster(sourceVdiskID, zeroStorClusterID, zeroStorClusterConf)
	confSource.SetTlogZeroStorCluster(targetVdiskID, zeroStorClusterID, zeroStorClusterConf)
	testCopy(t, confSource, dataShards, parityShards, blockSize, sourceVdiskID, targetVdiskID, privKey, false)
}

func testCopy(t *testing.T, confSource config.Source, dataShards, parityShards, blockSize int,
	sourceVdiskID, targetVdiskID, privKey string, diffCluster bool) {
	const (
		numLogs = 50
	)
	// generates some tlog data
	flusher, err := flusher.New(confSource, dataShards, parityShards, sourceVdiskID, privKey)
	require.NoError(t, err)

	for i := 0; i < numLogs; i++ {
		seq := uint64(i)
		idx := int64(i)
		data := make([]byte, blockSize)
		rand.Read(data)

		err = flusher.AddTransaction(schema.OpSet, seq, data, idx, tlog.TimeNowTimestamp())
		require.NoError(t, err)
	}
	_, err = flusher.Flush()
	require.NoError(t, err)

	// copy
	copier, err := newCopier(confSource, Config{
		SourceVdiskID: sourceVdiskID,
		TargetVdiskID: targetVdiskID,
		PrivKey:       privKey,
		DataShards:    dataShards,
		ParityShards:  parityShards,
		JobCount:      runtime.NumCPU(),
	})
	require.NoError(t, err)

	err = copier.Copy()
	require.NoError(t, err)

	// verify
	// - walk source, walk target
	// - match it
	var (
		sourceWrCh = make(chan *stor.WalkResult)
		now        = tlog.TimeNowTimestamp()
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for wr := range copier.storCliSource.Walk(0, now) {
			require.NoError(t, wr.Err)
			sourceWrCh <- wr
		}
	}()

	aggRefList := []string{sourceVdiskID, targetVdiskID}
	sourceRefList := []string{sourceVdiskID}
	targetRefList := []string{targetVdiskID}

	for wr := range copier.storCliTarget.Walk(0, now) {
		require.NoError(t, wr.Err)
		sourceWr := <-sourceWrCh
		// check the data
		require.Equal(t, sourceWr.Data, wr.Data)

		// check the reflist
		if !diffCluster {
			require.Equal(t, aggRefList, sourceWr.RefList)
			require.Equal(t, aggRefList, wr.RefList)
		} else {
			require.Equal(t, targetRefList, wr.RefList)
			require.Equal(t, sourceRefList, sourceWr.RefList)
		}
	}
	wg.Wait()
}
