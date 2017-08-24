package server

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

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
		DataShards:   1,
		ParityShards: 1,
		ListenAddr:   "127.0.0.1:0",
		FlushSize:    25,
		FlushTime:    10,
		PrivKey:      "12345678901234567890123456789012",
	}
)

func newZeroStorConfig(t *testing.T, vdiskID, privKey string,
	data, parity int) (func(), *config.StubSource, stor.Config) {
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
		Organization:    os.Getenv("iyo_organization"),
		Namespace:       "thedisk",
		IyoClientID:     os.Getenv("iyo_client_id"),
		IyoSecret:       os.Getenv("iyo_secret"),
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
	return cleanFunc, stubSource, storConf
}

// Test that we can send the data to tlog and decode it again correctly
func TestEndToEnd(t *testing.T) {
	const (
		expectedVdiskID = "1234567890"
		firstSequence   = 0
		numFlush        = 5
		dataLen         = 4096 * 4
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// config
	conf := testConf

	log.Infof("in memory redis pool")

	cleanFunc, stubSource, storConfig := newZeroStorConfig(t, expectedVdiskID, conf.PrivKey, conf.DataShards, conf.ParityShards)
	defer cleanFunc()

	// start the server
	s, err := NewServer(conf, stubSource)
	assert.Nil(t, err)

	go s.Listen(ctx)
	t.Logf("listen addr=%v", s.ListenAddr())

	// create tlog client
	client, err := tlogclient.New([]string{s.ListenAddr()}, expectedVdiskID, firstSequence, false)
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
			x := uint64(i)
			// check we can send it without error
			err := client.Send(schema.OpSet, x, int64(x), x, data)
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
	for wr := range storCli.Walk(0, uint64(time.Now().UnixNano())) {
		require.Nil(t, wr.Err)
		agg := wr.Agg

		assert.Equal(t, uint64(conf.FlushSize), agg.Size())

		vdiskID, err := agg.VdiskID()
		assert.Nil(t, err)
		assert.Equal(t, expectedVdiskID, vdiskID)

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

// Test tlog server ability to handle unordered message
func TestUnordered(t *testing.T) {
	const (
		vdiskID       = "12345"
		firstSequence = 10
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// config
	conf := testConf

	cleanFunc, stubSource, storConfig := newZeroStorConfig(t, vdiskID, conf.PrivKey, conf.DataShards, conf.ParityShards)
	defer cleanFunc()

	// start the server
	s, err := NewServer(conf, stubSource)
	assert.Nil(t, err)

	go s.Listen(ctx)

	t.Logf("listen addr=%v", s.ListenAddr())

	// create tlog client
	client, err := tlogclient.New([]string{s.ListenAddr()}, vdiskID, firstSequence, false)
	if !assert.Nil(t, err) {
		return
	}

	// initialize test data
	data := make([]byte, 4096)

	var wg sync.WaitGroup

	wg.Add(1)

	const numFlush = 4
	numLogs := conf.FlushSize * numFlush // number of logs to send.
	seqs := []uint64{}
	for i := 0; i < numLogs; i++ {
		seqs = append(seqs, uint64(i)+firstSequence)
	}

	// send tlog
	go func() {
		defer wg.Done()

		var seqIdx int
		for i := 0; i < numLogs; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// pick random sequence
				seqIdx = rand.Intn(len(seqs))

				seq := seqs[seqIdx]
				seqs = append(seqs[:seqIdx], seqs[seqIdx+1:]...)

				x := uint64(i)
				// check we can send it without error
				err := client.Send(schema.OpSet, seq, int64(x), x, data)
				if !assert.Nil(t, err) {
					cancelFunc()
					return
				}

				// send it twice, to test duplicated message
				err = client.Send(schema.OpSet, seq, int64(x), x, data)
				if !assert.Nil(t, err) {
					cancelFunc()
					return
				}
			}
		}
	}()

	// recv it
	respChan := client.Recv()
	testClientWaitSeqFlushed(ctx, t, respChan, cancelFunc, uint64(firstSequence+numLogs-1), false)

	wg.Wait()

	// decode it
	storCli, err := stor.NewClient(storConfig)
	require.Nil(t, err)

	var expectedSequence = uint64(firstSequence)

	for wr := range storCli.Walk(0, uint64(time.Now().UnixNano())) {
		require.Nil(t, wr.Err)
		agg := wr.Agg

		assert.Equal(t, uint64(conf.FlushSize), agg.Size())

		blocks, err := agg.Blocks()
		assert.Nil(t, err)

		assert.Equal(t, conf.FlushSize, blocks.Len())
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)

			require.Equal(t, expectedSequence, block.Sequence())
			expectedSequence++
		}
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
