package server

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

// TestResetFirstSequence tests the scenario
// of client resetting the vdisk's first sequence.
// Test flows:
// (1) Client #1 send  FlushSize+5  logs
// (2) Client #1 close the connection after FlushSize logs getting flushed
//		there will be 5 logs left in the buffer
// (3) create Client #2 and reset the first sequence to 0
// (4) Client #2 must receive the previous last 5 logs
// (5) Client #2 send another (n*FlushSize) logs
// (6) Client #2 must receive flush response for all of it's logs
func TestResetFirstSequence(t *testing.T) {
	// config
	conf := testConf
	conf.FlushTime = 100 // make it high value to avoid flush by timeout

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	s, err := NewServer(conf, poolFactory)
	assert.Nil(t, err)

	go s.Listen()

	t.Logf("listen addr=%v", s.ListenAddr())
	const (
		additionalLogs = 5
		vdiskID        = "12345"
	)
	firstNumLogs := conf.FlushSize + additionalLogs

	// Step #1
	// create tlog client
	client1, err := tlogclient.New(s.ListenAddr(), vdiskID, 0, false)
	if !assert.Nil(t, err) {
		return
	}

	// initialize test data
	data := make([]byte, 4096)

	testClientSendWaitFlushResp(t, client1, client1.Recv(), firstNumLogs, 0, uint64(conf.FlushSize)-1, data)

	// Step #2
	client1.Close()

	// Step #3
	client2, err := tlogclient.New(s.ListenAddr(), vdiskID, 0, true)
	if !assert.Nil(t, err) {
		return
	}

	resp2Chan := client2.Recv()
	// Step #4
	for re := range resp2Chan {
		if re.Resp != nil && re.Resp.Status == tlog.BlockStatusFlushOK {
			seqs := re.Resp.Sequences
			if seqs[len(seqs)-1] == uint64(firstNumLogs)-1 {
				break
			}
		}
	}

	// step #5 and #6
	testClientSendWaitFlushResp(t, client2, resp2Chan, conf.FlushSize, 0, uint64(conf.FlushSize)-1, data)
}

// this func provide this flows:
// - client send `numLogs` of logs starting from startSeq
// - client wait until at least `lastSeqToFlush` getting flushed
func testClientSendWaitFlushResp(t *testing.T, c *tlogclient.Client, respChan <-chan *tlogclient.Result,
	numLogs int, startSeq, lastSeqToFlush uint64, data []byte) {

	var wg sync.WaitGroup

	wg.Add(2)

	ctx, cancelFunc := context.WithCancel(context.Background())

	// recv it
	go func() {
		defer wg.Done()
		testClientWaitSeqFlushed(ctx, t, respChan, cancelFunc, lastSeqToFlush, false)
	}()

	// send tlog
	go func() {
		defer wg.Done()
		testClientSendLog(ctx, t, c, cancelFunc, startSeq, numLogs, data)
	}()

	wg.Wait()
}

// wait for sequence seqWait to be flushed
func testClientWaitSeqFlushed(ctx context.Context, t *testing.T, respChan <-chan *tlogclient.Result,
	cancelFunc func(), seqWait uint64, exactSeq bool) {

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

				if exactSeq && seq == seqWait {
					return
				}
				if !exactSeq && seq >= seqWait { // we've received all sequences
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}

}

// send `numLogs` of logs, starting from sequence number=seq
func testClientSendLog(ctx context.Context, t *testing.T, c *tlogclient.Client, cancelFunc func(),
	startSeq uint64, numLogs int, data []byte) {

	for i := startSeq; i < uint64(numLogs); i++ {
		select {
		case <-ctx.Done():
			return
		default:
			x := uint64(i)
			err := c.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
			if !assert.Nil(t, err) {
				cancelFunc()
				return
			}
		}
	}

}
