package server

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

var (
	testConf = &Config{
		ListenAddr: "127.0.0.1:0",
		FlushSize:  25,
		FlushTime:  10,
		PrivKey:    "12345678901234567890123456789012",
	}
)

const (
	testDataShards   = 1
	testParityShards = 1
)

func newZeroStorConfig(t *testing.T, vdiskID, privKey string) (func(), *config.StubSource, stor.Config) {
	// stor server
	storCluster, err := embeddedserver.NewZeroStorCluster(testDataShards + testParityShards)
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
		IyoClientID:     "",
		IyoSecret:       "",
		ZeroStorShards:  storCluster.Addrs(),
		MetaShards:      []string{mdServer.ListenAddr()},
		DataShardsNum:   testDataShards,
		ParityShardsNum: testParityShards,
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
		DataShards:   testDataShards,
		ParityShards: testParityShards,
	})

	cleanFunc := func() {
		mdServer.Stop()
		storCluster.Close()
	}
	return cleanFunc, stubSource, storConf
}

// Test that we can send the data to tlog and decode it again correctly
func TestEndToEnd(t *testing.T) {
	const (
		expectedVdiskID = "1234567890"
		numFlush        = 5
		dataLen         = 4096 * 4
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// config
	conf := testConf

	log.Infof("in memory redis pool")

	cleanFunc, stubSource, storConfig := newZeroStorConfig(t, expectedVdiskID, conf.PrivKey)
	defer cleanFunc()

	// start the server
	s, err := NewServer(conf, stubSource)
	assert.Nil(t, err)

	go s.Listen(ctx)
	t.Logf("listen addr=%v", s.ListenAddr())

	// create tlog client
	client, err := tlogclient.New([]string{s.ListenAddr()}, expectedVdiskID)
	if !assert.Nil(t, err) {
		return
	}

	// initialize test data

	data := make([]byte, dataLen)
	for i := 0; i < (dataLen); i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	var wg sync.WaitGroup

	wg.Add(2)

	numLogs := conf.FlushSize * numFlush // number of logs to send.

	// send tlog
	go func() {
		defer wg.Done()
		for i := 0; i < numLogs; i++ {
			x := uint64(i) + 1
			// check we can send it without error
			err := client.Send(schema.OpSet, x, int64(x), int64(x), data)
			assert.Nil(t, err)
		}
	}()

	// recv it
	go func() {
		defer wg.Done()
		expected := numLogs + numFlush
		respChan := client.Recv()
		for i := 0; i < expected; i++ {
			re := <-respChan
			if !assert.Nil(t, re.Err) {
				continue
			}
			assert.Equal(t, true, re.Resp.Status > 0)

			respStatus := tlog.BlockStatus(re.Resp.Status)
			if respStatus == tlog.BlockStatusFlushOK {
				assert.Equal(t, conf.FlushSize, len(re.Resp.Sequences))
			} else if respStatus == tlog.BlockStatusRecvOK {
				assert.Equal(t, 1, len(re.Resp.Sequences))
			}
		}
	}()

	wg.Wait()

	// decode it
	storCli, err := stor.NewClient(storConfig)
	require.Nil(t, err)

	aggReceived := 0
	for wr := range storCli.Walk(0, tlog.TimeNowTimestamp()) {
		require.Nil(t, wr.Err)
		agg := wr.Agg

		assert.Equal(t, uint64(conf.FlushSize), agg.Size())

		blocks, err := agg.Blocks()
		assert.Nil(t, err)

		assert.Equal(t, conf.FlushSize, blocks.Len())
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)

			// check the data content
			blockData, err := block.Data()
			require.Nil(t, err)
			require.Equal(t, data, blockData)
		}

		aggReceived++
	}

	require.Equal(t, numFlush, aggReceived)
}

func init() {
	log.SetLevel(log.DebugLevel)
}
