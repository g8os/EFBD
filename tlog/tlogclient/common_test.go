package tlogclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

func testClientSend(t *testing.T, client *Client, startSeq, endSeq uint64, data []byte) {
	for x := startSeq; x <= endSeq; x++ {
		err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
		assert.Nil(t, err)
	}

}
func testClientWaitSeqFlushed(ctx context.Context, t *testing.T, respChan <-chan *Result,
	cancelFunc func(), seqWait uint64) (finished bool) {

	for {
		select {
		case <-time.After(20 * time.Second):
			t.Fatal("testClientWaitSeqFlushed timeout")
		case re := <-respChan:
			if re.Err != nil {
				t.Logf("recv err = %v", re.Err)
				return
			}
			status := re.Resp.Status
			if status < 0 {
				continue
			}

			if status == tlog.BlockStatusFlushOK {
				seqs := re.Resp.Sequences
				seq := seqs[len(seqs)-1]

				if seq >= seqWait { // we've received all sequences
					finished = true
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
