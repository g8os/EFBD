package tlogclient

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

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
func TestMultipleServerBasic(t *testing.T) {
	log.Info("TestMultipleServerBasic")

	conf := testMultipleServerBasicConf()
	conf.masterStopped = true
	testTwoServers(t, conf)
}

func TestMultipleServerBasicMasterFailToFlush(t *testing.T) {
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
		firstSendStartSeq:  0,
		firstSendEndSeq:    99,
		firstWaitEndSeq:    99,
		secondSendStartSeq: 100,
		secondSendEndSeq:   199,
		secondWaitEndSeq:   199,
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
	testTwoServers(t, conf)

}

func TestMultipleServerResendHotReload(t *testing.T) {
	log.Info("TestMultipleServerResendHotReload")

	conf := testMultipleServerResendUnflushedConf()
	conf.hotReload = true
	testTwoServers(t, conf)
}

func testMultipleServerResendUnflushedConf() testTwoServerConf {
	return testTwoServerConf{
		firstSendStartSeq:  0,
		firstSendEndSeq:    90,
		firstWaitEndSeq:    74,
		secondSendStartSeq: 91,
		secondSendEndSeq:   199,
		secondWaitEndSeq:   199,
	}
}

func testTwoServers(t *testing.T, ttConf testTwoServerConf) {
	const (
		vdiskID  = "myimg"
		firstSeq = 0
		numLogs1 = 100
	)
	data := make([]byte, 4096)

	ctx1, cancelFunc1 := context.WithCancel(context.Background())
	defer cancelFunc1()

	ctx2, cancelFunc2 := context.WithCancel(context.Background())
	defer cancelFunc2()

	stors, err := newRedisStors(vdiskID)
	assert.Nil(t, err)
	defer func() {
		for _, stor := range stors {
			stor.Close()
		}
	}()

	t.Log("Create tlog servers")
	pool1, _, t1, err := createTestTlogServer(ctx1, vdiskID, stors)
	assert.Nil(t, err)
	defer pool1.Close()

	pool2, tlogConf, t2, err := createTestTlogServer(ctx2, vdiskID, stors)
	assert.Nil(t, err)
	defer pool2.Close()

	t.Log("connect client")

	tlogAddrs := []string{t1.ListenAddr(), t2.ListenAddr()}
	if ttConf.hotReload {
		tlogAddrs = []string{t1.ListenAddr()}
	}

	client, err := New(tlogAddrs, vdiskID, 0, false)
	assert.Nil(t, err)

	respChan := client.Recv()

	t.Log("write data to server #1")

	seqs := make(map[uint64]struct{})
	for i := ttConf.firstSendStartSeq; i <= ttConf.firstSendEndSeq; i++ {
		seqs[i] = struct{}{}
	}

	go func() {
		testClientSend(t, client, ttConf.firstSendStartSeq, ttConf.firstSendEndSeq, data)
	}()

	// wait for it to be flushed
	testClientWaitSeqFlushed(ctx1, t, respChan, cancelFunc1, ttConf.firstWaitEndSeq)

	switch {
	case ttConf.masterStopped:
		// simulate stopping server 1
		// - remove server 1 from client's addrs and disconnect the socket
		client.addrs = client.addrs[1:]
		client.conn.Close()

	case ttConf.masterFailedToFlush:
		// simulate master failed to flush
		pool1.Close()
	case ttConf.hotReload:
		cancelFunc1()
		client.ChangeServerAddrs([]string{t2.ListenAddr()})
	}

	t.Log("write data to server #2")
	for i := ttConf.secondSendStartSeq; i <= ttConf.secondSendEndSeq; i++ {
		seqs[i] = struct{}{}
	}

	go func() {
		testClientSend(t, client, ttConf.secondSendStartSeq, ttConf.secondSendEndSeq, data)
	}()

	// wait for it to be flushed
	if !assert.True(t, testClientWaitSeqFlushed(ctx2, t, respChan, cancelFunc1, ttConf.secondWaitEndSeq)) {
		return
	}

	// validate with the decoder
	flushedAll, err := validateWithDecoder(seqs, pool2, tlogConf.K, tlogConf.M, vdiskID,
		tlogConf.PrivKey, tlogConf.HexNonce, ttConf.firstSendStartSeq, ttConf.secondSendEndSeq)
	assert.Nil(t, err)
	assert.True(t, flushedAll)
}

func validateWithDecoder(seqs map[uint64]struct{}, pool tlog.RedisPool, k, m int,
	vdiskID, privKey, hexNonce string, startSeq, endSeq uint64) (bool, error) {

	dec, err := decoder.New(pool, k, m, vdiskID, privKey, hexNonce)
	if err != nil {
		return false, err
	}

	aggChan := dec.Decode(decoder.NewLimitBySequence(startSeq, endSeq))
	for da := range aggChan {
		if da.Err != nil {
			break
		}
		agg := da.Agg
		blocks, err := agg.Blocks()
		if err != nil {
			return false, err
		}
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			delete(seqs, block.Sequence())
		}
	}
	return len(seqs) == 0, nil
}

func createTestTlogServer(ctx context.Context, vdiskID string,
	stors []*redisstub.MemoryRedis) (tlog.RedisPool, *server.Config, *server.Server, error) {
	conf := server.DefaultConfig()
	conf.ListenAddr = ""
	conf.K = 1
	conf.M = 1
	conf.FlushSize = 25

	addrs := []string{}
	for _, stor := range stors {
		addrs = append(addrs, stor.Address())
	}
	serverConfigs, err := config.ParseCSStorageServerConfigStrings(strings.Join(addrs, ","))
	if err != nil {
		return nil, nil, nil, err
	}

	// create any kind of valid pool factory
	poolFact, err := tlog.AnyRedisPoolFactory(ctx, tlog.RedisPoolFactoryConfig{
		RequiredDataServerCount: len(stors),
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           true,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	pool, err := poolFact.NewRedisPool(vdiskID)
	if err != nil {
		return nil, nil, nil, err
	}

	server, err := server.NewServer(conf, poolFact)
	if err != nil {
		return nil, nil, nil, err
	}

	go server.Listen(ctx)
	return pool, conf, server, nil
}

func newRedisStors(vdiskID string) (stors []*redisstub.MemoryRedis, err error) {
	stor1 := redisstub.NewMemoryRedis()
	stor2 := redisstub.NewMemoryRedis()

	stors = append(stors, stor1, stor2)

	go stor1.Listen()
	go stor2.Listen()

	return
}
