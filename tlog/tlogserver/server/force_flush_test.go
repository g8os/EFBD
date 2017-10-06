package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

// Test server's force flush feature
// Steps:
// 1. set flushTime to very high value to avoid flush by timeout
// 2. sequence that will be force flushed must not be multiple of FlushSize
// 3. client force flushed that sequence
// 4. create goroutine to wait for force flushed seq
// 5. client send the logs
func TestForceFlushAtSeq(t *testing.T) {
	const (
		vdiskID       = "1234567890"
		firstSequence = 1
	)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Step #1
	// create and start server
	conf := testConf
	conf.FlushTime = 1000
	cleanFunc, stubSource, _ := newZeroStorConfig(t, vdiskID, conf.PrivKey, conf.DataShards, conf.ParityShards)
	defer cleanFunc()

	// start the server
	s, err := NewServer(conf, stubSource)
	require.Nil(t, err)

	go s.Listen(ctx)

	t.Logf("listen addr=%v", s.ListenAddr())

	// #Step 2
	numLogs := conf.FlushSize + 10

	// sequence where we want to do force flush.
	// we force flush before numLogs to test that the flushing
	// happens at that sequence, not after
	forceFlushedSeq := uint64(numLogs - 5)

	// create tlog client
	client, _, err := tlogclient.New([]string{s.ListenAddr()}, vdiskID)
	require.Nil(t, err)

	// Step 3
	err = client.ForceFlushAtSeq(forceFlushedSeq)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	respChan := client.Recv()

	// Step #4
	go func() {
		defer wg.Done()
		testClientWaitSeqFlushed(ctx, t, respChan, cancelFunc, forceFlushedSeq, true)
	}()

	// Step #5
	go func() {
		defer wg.Done()
		data := make([]byte, 4096)
		testClientSendLog(ctx, t, client, cancelFunc, 0, numLogs, data)
	}()

	wg.Wait()

	// we still have some blocks to be flushed, but it is not important for this test
}

// Test server's force flush feature with possible race condition
// Steps:
// 1. set flushTime to very high value to avoid flush by timeout
// 2. sequence that will be force flushed must not be multiple of FlushSize
//   and musti be last sequence
// 3. create goroutine to wait for force flushed seq
// 4. client send the logs
// 5. client force flushed that sequence
func TestForceFlushAtSeqPossibleRace(t *testing.T) {
	const (
		vdiskID       = "12345"
		firstSequence = 1
	)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Step #1
	// create and start server
	conf := testConf
	conf.FlushTime = 1000
	cleanFunc, stubSource, _ := newZeroStorConfig(t, vdiskID, conf.PrivKey, conf.DataShards, conf.ParityShards)
	defer cleanFunc()

	// start the server
	s, err := NewServer(conf, stubSource)
	require.Nil(t, err)

	go s.Listen(ctx)

	t.Logf("listen addr=%v", s.ListenAddr())

	// #Step 2
	numLogs := conf.FlushSize + 10

	// sequence where we want to do force flush.
	// we force flush before numLogs to test that the flushing
	// happens at that sequence, not after
	forceFlushedSeq := uint64(numLogs - 1)

	// create tlog client
	client, _, err := tlogclient.New([]string{s.ListenAddr()}, vdiskID)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	respChan := client.Recv()
	// Step #3
	go func() {
		defer wg.Done()
		testClientWaitSeqFlushed(ctx, t, respChan, cancelFunc, forceFlushedSeq, false)
	}()

	// Step #4
	data := make([]byte, 4096)
	testClientSendLog(ctx, t, client, cancelFunc, 0, numLogs, data)

	// Step 5
	err = client.ForceFlushAtSeq(forceFlushedSeq)
	require.Nil(t, err)

	ended := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		ended <- struct{}{}
	}()

	select {
	case <-time.After(3 * time.Second):
		t.Fatalf("TestForceFlushAtSeqPossibleRace failed. too long")
	case <-ended:
		t.Logf("TestForceFlushAtSeqPossibleRace succeed")
		break
	}
}
