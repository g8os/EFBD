package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient"
)

// Test server's force flush feature
func TestForceFlush(t *testing.T) {
	// config
	conf := testConf
	conf.FlushTime = 1000 // set flush time to high value to avoid flush by timeout to be executed

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	s, err := NewServer(conf, poolFactory)
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
	client, err := tlogclient.New(s.ListenAddr(), vdiskID, firstSequence)
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
				if !assert.Equal(t, true, re.Resp.Status > 0) {
					cancelFunc()
					return
				}

				if re.Resp.Status == tlog.BlockStatusFlushOK {
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
							case <-time.After(5 * time.Second):
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
