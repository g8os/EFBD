package server

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

// scenario
// - t1
// 		- t1 start and listen to coord port X
// 		- c1 connect to t1
// 		- c1 send some transactions (A)
// - t2 start and connect to coord port X
// 		- c2 connect to t2
// - t1 dies
// - c2 receive status ready
// - c2 send some transaction (B)
// -  make sure we have (A+B) with proper sequence number
func TestCoord(t *testing.T) {
	tlogConf := DefaultConfig()
	tlogConf.ListenAddr = ""

	const (
		vdiskID = "12345"
	)

	tlogConf.WaitListenAddr = WaitListenAddrRandom

	cleanFunc, confSource, _ := newZeroStorConfig(t, vdiskID, tlogConf.PrivKey,
		tlogConf.DataShards, tlogConf.ParityShards)
	defer cleanFunc()

	parentCtx, parentCancelFunc := context.WithCancel(context.Background())
	defer parentCancelFunc()

	ctx1, cancelFunc1 := context.WithCancel(parentCtx)
	defer cancelFunc1()

	var (
		wg1     sync.WaitGroup
		wg2     sync.WaitGroup
		numLogA = tlogConf.FlushSize
		numLogB = tlogConf.FlushSize
	)
	// start tlogserver t1
	t1, err := NewServer(tlogConf, confSource)
	require.NoError(t, err)

	go t1.Listen(ctx1)

	// start client 1
	c1, err := tlogclient.New([]string{t1.ListenAddr()}, vdiskID)
	require.NoError(t, err)

	resp1Ch := c1.Recv()
	// start goroutine for c1 to wait until A flushed
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		testClientWaitSeqFlushed(ctx1, t, resp1Ch, cancelFunc1, uint64(numLogA), true)
	}()

	data := make([]byte, 4096)
	rand.Read(data)
	// c1 : send A transactions
	for i := tlog.FirstSequence; i <= uint64(numLogA); i++ {
		err = c1.Send(schema.OpSet, i, int64(i), int64(i), data)
	}

	// start t2
	ctx2, cancelFunc2 := context.WithCancel(parentCtx)
	defer cancelFunc2()

	// set t2 to connect to t1
	tlogConf.WaitConnectAddr = t1.WaitListenAddr()
	require.NotEmpty(t, tlogConf.WaitConnectAddr)

	t2, err := NewServer(tlogConf, confSource)
	require.NoError(t, err)

	go t2.Listen(ctx2)

	// start c2
	c2, err := tlogclient.New([]string{t2.ListenAddr()}, vdiskID)
	require.NoError(t, err)

	// wait for t1 finish it flush and kil it
	wg1.Wait()
	c1.Close()
	cancelFunc1()

	// start c2 receiver
	resp2Ch := c2.Recv()
	c2.WaitReady()

	wg2.Add(1)
	// start goroutine for c1 to wait until A flushed
	go func() {
		defer wg2.Done()
		testClientWaitSeqFlushed(ctx2, t, resp2Ch, cancelFunc2, uint64(numLogB+numLogA), true)
	}()

	c2LastSeq := c2.LastFlushedSequence()
	for i := c2LastSeq + 1; i <= uint64(numLogB)+c2LastSeq; i++ {
		err = c2.Send(schema.OpSet, i, int64(i), int64(i), data)
	}
	wg2.Wait()

	// make sure we have A+B with proper sequence number
	storCli, err := stor.NewClientFromConfigSource(confSource, vdiskID, tlogConf.PrivKey, tlogConf.DataShards,
		tlogConf.ParityShards)
	require.NoError(t, err)

	seq := tlog.FirstSequence
	for wr := range storCli.Walk(0, tlog.TimeNowTimestamp()) {
		require.NoError(t, wr.Err)
		agg := wr.Agg
		blocks, err := agg.Blocks()
		require.NoError(t, err)

		blockNum := int(agg.Size())
		for i := 0; i < blockNum; i++ {
			require.Equal(t, seq, blocks.At(i).Sequence())
			seq++
		}
	}
	require.Equal(t, uint64(numLogA+numLogB), seq-1)
}
