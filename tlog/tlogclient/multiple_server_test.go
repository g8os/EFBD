package tlogclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/meta/embedserver"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

type testTwoServerConf struct {
	firstSendStartSeq uint64
	firstSendEndSeq   uint64
	firstWaitEndSeq   uint64

	secondSendStartSeq uint64
	secondSendEndSeq   uint64
	secondWaitEndSeq   uint64

	// master stopped simulated with:
	// - removing master from the client's address
	// - close the socket
	masterStopped bool

	// master failed to flush
	// simulated by closing the redis pool
	masterFailedToFlush bool

	// simulated by:
	// - only use one tlog address
	// - stop tlog 1
	// - add address of tlog 2
	hotReload bool
}

// Test client with two tlog servers in normal condition:
// - tlog 1 flush all it receive
// - tlog1 lost/destroyed
// - tlog 2 continue the work
// Start 2 tlogservers with same storage, flushSize = 25
// start client,
// - connect to server
// - send log 1 - 100
// - wait until all flushed
// stop tlog server 1 // or remove it from client addrs and then disconnect
// - send log 101-200
// - wait until all flushed
// Start tlog decoder
// - make sure all sequence successfully decoded
func TestMultipleServerBasicMasterStop(t *testing.T) {
	log.Info("TestMultipleServerBasic")

	conf := testMultipleServerBasicConf()
	conf.masterStopped = true
	testTwoServers(t, conf)
}

func testMultipleServerBasicMasterFailToFlush(t *testing.T) {
	log.Info("TestMultipleServerBasicMasterFailToFlush")

	conf := testMultipleServerBasicConf()
	conf.masterFailedToFlush = true
	testTwoServers(t, conf)
}

func TestMultipleServerBasicHotReload(t *testing.T) {
	log.Info("TestMultipleServerBasicHotReload")

	conf := testMultipleServerBasicConf()
	conf.hotReload = true
	testTwoServers(t, conf)
}

func testMultipleServerBasicConf() testTwoServerConf {
	return testTwoServerConf{
		firstSendStartSeq:  1,
		firstSendEndSeq:    100,
		firstWaitEndSeq:    100,
		secondSendStartSeq: 101,
		secondSendEndSeq:   200,
		secondWaitEndSeq:   200,
	}
}

// TestMultipleServerResendUnflushed test multiple servers
// in case the 1st tlog server has some unflushed blocks
func TestMultipleServerResendUnflushed(t *testing.T) {
	log.Info("TestMultipleServerResendUnflushed")

	conf := testMultipleServerResendUnflushedConf()
	conf.masterStopped = true
	testTwoServers(t, conf)
}

func TestMultipleServerResendUnflushedMasterFailFlush(t *testing.T) {
	log.Info("TestMultipleServerResendUnflushedMasterFailFlush")

	conf := testMultipleServerResendUnflushedConf()
	conf.masterFailedToFlush = true

}

func TestMultipleServerResendHotReload(t *testing.T) {
	log.Info("TestMultipleServerResendHotReload")

	conf := testMultipleServerResendUnflushedConf()
	conf.hotReload = true
	testTwoServers(t, conf)
}

func testMultipleServerResendUnflushedConf() testTwoServerConf {
	return testTwoServerConf{
		firstSendStartSeq:  1,
		firstSendEndSeq:    91,
		firstWaitEndSeq:    75,
		secondSendStartSeq: 92,
		secondSendEndSeq:   200,
		secondWaitEndSeq:   200,
	}
}

func testTwoServers(t *testing.T, ttConf testTwoServerConf) {
	const (
		vdiskID  = "myimg"
		firstSeq = 1
		numLogs1 = 100
	)
	data := make([]byte, 4096)

	// tlog conf
	tlogConf := testConf
	tlogConf.DataShards = 1
	tlogConf.ParityShards = 1
	tlogConf.FlushSize = 25

	storCluster, err := embeddedserver.NewZeroStorCluster(tlogConf.DataShards + tlogConf.ParityShards)
	require.Nil(t, err)
	defer storCluster.Close()

	mdServer, err := embedserver.New()
	defer mdServer.Stop()

	ctx1, cancelFunc1 := context.WithCancel(context.Background())
	defer cancelFunc1()

	ctx2, cancelFunc2 := context.WithCancel(context.Background())
	defer cancelFunc2()

	t.Log("Create tlog servers")
	t1, _ := createTestTlogServer(ctx1, t, tlogConf, storCluster, mdServer, vdiskID)

	t2, storConf2 := createTestTlogServer(ctx2, t, tlogConf, storCluster, mdServer, vdiskID)

	t.Log("connect client")

	tlogAddrs := []string{
		t1.ListenAddr(),
		t2.ListenAddr(),
	}
	if ttConf.hotReload {
		tlogAddrs = tlogAddrs[0:1]
	}

	client, _, err := New(tlogAddrs, vdiskID)
	require.Nil(t, err)

	respChan := client.Recv()

	log.Info("write data to server #1")

	seqs := make(map[uint64]struct{})
	for i := ttConf.firstSendStartSeq; i <= ttConf.firstSendEndSeq; i++ {
		seqs[i] = struct{}{}
	}

	go func() {
		testClientSend(t, client, ttConf.firstSendStartSeq, ttConf.firstSendEndSeq, data)
	}()

	// wait for it to be flushed
	testClientWaitSeqFlushed(ctx1, t, respChan, cancelFunc1, ttConf.firstWaitEndSeq)

	log.Info("simulate failover scenario")
	switch {
	case ttConf.masterStopped:
		// simulate stopping server 1
		// - remove server 1 from client's addrs and disconnect the socket
		client.servers = client.servers[1:]
		client.conn.Close()

	case ttConf.masterFailedToFlush:
		// simulate master failed to flush
		//storCluster1.Close()
	case ttConf.hotReload:
		cancelFunc1()
		client.ChangeServerAddresses([]string{
			t2.ListenAddr(),
		})
	}

	log.Info("write data again, should be handled by server #2")
	for i := ttConf.secondSendStartSeq; i <= ttConf.secondSendEndSeq; i++ {
		seqs[i] = struct{}{}
	}

	go func() {
		testClientSend(t, client, ttConf.secondSendStartSeq, ttConf.secondSendEndSeq, data)
	}()

	// wait for it to be flushed
	require.True(t, testClientWaitSeqFlushed(ctx2, t, respChan, cancelFunc1, ttConf.secondWaitEndSeq))

	// validate with the decoder
	validateWithDecoder(t, seqs, storConf2, ttConf.firstSendStartSeq, ttConf.secondSendEndSeq)
}

func validateWithDecoder(t *testing.T, seqs map[uint64]struct{}, storConf stor.Config,
	startSeq, endSeq uint64) {

	storCli, err := stor.NewClient(storConf)
	require.Nil(t, err)

	for wr := range storCli.Walk(0, tlog.TimeNowTimestamp()) {
		require.Nil(t, wr.Err)
		agg := wr.Agg

		blocks, err := agg.Blocks()
		require.Nil(t, err)

		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			delete(seqs, block.Sequence())
		}
	}
	// flushed all
	require.Equal(t, 0, len(seqs))
}

func createTestTlogServer(ctx context.Context, t *testing.T, conf *server.Config,
	storCluster *embeddedserver.ZeroStorCluster,
	mdServer *embedserver.Server, vdiskID string) (*server.Server, stor.Config) {

	_, configSource, storConf := newZeroStorConfigFromCluster(t, storCluster, mdServer, vdiskID,
		conf.PrivKey, conf.DataShards, conf.ParityShards)

	tlogServer, err := server.NewServer(conf, configSource)
	require.Nil(t, err)

	go tlogServer.Listen(ctx)
	return tlogServer, storConf
}
