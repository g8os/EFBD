package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

// Test server's force flush feature
func TestForceFlush(t *testing.T) {
	// create and start server
	s, _, err := createTestServer(t, 1000)
	assert.Nil(t, err)

	go s.Listen()

	t.Logf("listen addr=%v", s.ListenAddr())
	const (
		vdiskID          = "12345"
		firstSequence    = 0
		forceFlushPeriod = 5
		numLogs          = forceFlushPeriod * 20
	)

	// create tlog client
	client, err := tlogclient.New(s.ListenAddr(), vdiskID, firstSequence, false)
	if !assert.Nil(t, err) {
		return
	}

	// initialize test data
	data := make([]byte, 4096)
	var wg sync.WaitGroup

	wg.Add(2)
	lastSeqFlushedChan := make(chan uint64, 1) // channel of last sequence flushed

	ctx, cancelFunc := context.WithCancel(context.Background())

	// recv it
	go func() {
		defer wg.Done()

		respChan := client.Recv()
		for {
			select {
			case re := <-respChan:
				if !assert.Nil(t, re.Err) {
					cancelFunc()
					return
				}
				status := re.Resp.Status
				if !assert.Equal(t, true, status > 0) {
					cancelFunc()
					return
				}

				if status == tlog.BlockStatusFlushOK || status == tlog.BlockStatusForceFlushReceived {
					seqs := re.Resp.Sequences
					seq := seqs[len(seqs)-1]
					lastSeqFlushedChan <- seq

					if seq == uint64(numLogs)-1 { // we've received all sequences
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// send tlog
	go func() {
		defer wg.Done()

		for i := 0; i < numLogs+1; i++ { // need +1 because we need last sequence to be force flushed
			select {
			case <-ctx.Done():
				return
			default:
				if i != 0 && i%forceFlushPeriod == 0 {
					func() {
						for {
							// wait for all sent sequences to be flushed
							err := client.ForceFlush()
							if !assert.Nil(t, err) {
								return
							}
							select {
							case lastSeqFlushed := <-lastSeqFlushedChan:
								if lastSeqFlushed >= uint64(i)-1 {
									return
								}
							case <-time.After(2 * time.Second):
								// resend force flush if time out after few seconds
							}
						}
					}()
				}
				if i == numLogs {
					return
				}

				x := uint64(i)
				err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
				if !assert.Nil(t, err) {
					cancelFunc()
					return
				}
			}
		}
	}()

	wg.Wait()
}

// Test server's force flush feature
// Steps:
// 1. set flushTime to very high value to avoid flush by timeout
// 2. sequence that will be force flushed must not be multiple of FlushSize
// 3. client force flushed that sequence
// 4. create goroutine to wait for force flushed seq
// 5. client send the logs
func TestForceFlushAtSeq(t *testing.T) {
	// Step #1
	// create and start server
	s, conf, err := createTestServer(t, 1000)
	assert.Nil(t, err)

	go s.Listen()

	t.Logf("listen addr=%v", s.ListenAddr())
	const (
		vdiskID       = "12345"
		firstSequence = 0
	)

	// #Step 2
	numLogs := conf.FlushSize + 10

	// sequence where we want to do force flush.
	// we force flush before numLogs to test that the flushing
	// happens at that sequence, not after
	forceFlushedSeq := uint64(numLogs - 5)

	// create tlog client
	client, err := tlogclient.New(s.ListenAddr(), vdiskID, firstSequence, false)
	if !assert.Nil(t, err) {
		return
	}

	// Step 3
	err = client.ForceFlushAtSeq(forceFlushedSeq)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	ctx, cancelFunc := context.WithCancel(context.Background())

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

func TestForceFlushAtSeqPossibleRace(t *testing.T) {
	for i := 0; i < 6; i++ {
		testForceFlushAtSeqPossibleRace(t, i%2 == 0)
	}
}

// Test server's force flush feature with possible race condition
// Steps:
// 1. set flushTime to very high value to avoid flush by timeout
// 2. sequence that will be force flushed must not be multiple of FlushSize
//   and musti be last sequence
// 3. create goroutine to wait for force flushed seq
// 4. client send the logs
// 5. client force flushed that sequence
func testForceFlushAtSeqPossibleRace(t *testing.T, withSleep bool) {
	// Step #1
	// create and start server
	s, conf, err := createTestServer(t, 1000)
	assert.Nil(t, err)

	go s.Listen()

	t.Logf("listen addr=%v", s.ListenAddr())
	const (
		vdiskID       = "12345"
		firstSequence = 0
	)

	// #Step 2
	numLogs := conf.FlushSize + 10

	// sequence where we want to do force flush.
	// we force flush before numLogs to test that the flushing
	// happens at that sequence, not after
	forceFlushedSeq := uint64(numLogs - 1)

	// create tlog client
	client, err := tlogclient.New(s.ListenAddr(), vdiskID, firstSequence, false)
	if !assert.Nil(t, err) {
		return
	}

	var wg sync.WaitGroup
	ctx, cancelFunc := context.WithCancel(context.Background())
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
	if withSleep {
		// to avoid race condition
		time.Sleep(1 * time.Second)
	}
	err = client.ForceFlushAtSeq(forceFlushedSeq)
	assert.Nil(t, err)

	ended := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		ended <- struct{}{}
	}()

	select {
	case <-time.After(3 * time.Second):
		t.Fatalf("TestForceFlushAtSeqPossibleRace failed. too long, withSleep =%v", withSleep)
	case <-ended:
		t.Logf("TestForceFlushAtSeqPossibleRace succeed, with sleep=%v", withSleep)
		break
	}
}
