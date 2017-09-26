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
				continue
			}

			if status == tlog.BlockStatusFlushOK {
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

// send `numLogs` of logs, starting from sequence number=seq
func testClientSendLog(ctx context.Context, t *testing.T, c *tlogclient.Client, cancelFunc func(),
	startSeq uint64, numLogs int, data []byte) {

	for i := startSeq; i < uint64(numLogs); i++ {
		select {
		case <-ctx.Done():
			return
		default:
			x := uint64(i)
			err := c.Send(schema.OpSet, x, int64(x), int64(x), data)
			if !assert.Nil(t, err) {
				cancelFunc()
				return
			}
		}
	}

}
