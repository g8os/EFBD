package tlogclient

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogserver/server"
)

// TestSimpleReconnect test that client can connect again after getting disconnected
func TestSimpleReconnect(t *testing.T) {
	const (
		vdisk         = "12345"
		firstSequence = 0
		numLogs       = 100
	)

	serv, _, err := createTestServer()
	assert.Nil(t, err)
	go serv.Listen()

	client, err := New(serv.ListenAddr(), vdisk, firstSequence)
	assert.Nil(t, err)

	data := make([]byte, 4096)

	var wg sync.WaitGroup

	// start receiver goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitForBlockReceivedResponse(t, client, 0, uint64(numLogs)-1)
	}()

	// send tlog, and disconnect it few times in the middle to test re-connect ability.
	for i := 0; i < numLogs; i++ {
		x := uint64(i)

		if i%5 == 0 {
			client.conn.Close() // simulate closed connection by closing the socket
		}

		// send
		err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
		assert.Nil(t, err)
	}

	wg.Wait()
}

func waitForBlockReceivedResponse(t *testing.T, client *Client, minSequence, maxSequence uint64) {
	// map of sequence we want to wait for the response to come
	logsToRecv := map[uint64]struct{}{}
	for i := minSequence; i <= maxSequence; i++ {
		logsToRecv[i] = struct{}{}
	}

	respChan := client.Recv(1)

	for len(logsToRecv) > 0 {
		// recv
		resp := <-respChan

		if resp.Err == nil {
			// check response content
			if resp.Resp != nil && resp.Resp.Status == tlog.BlockStatusRecvOK {
				assert.Equal(t, 1, len(resp.Resp.Sequences))
				seqResp := resp.Resp.Sequences[0]

				if _, ok := logsToRecv[seqResp]; ok {
					delete(logsToRecv, seqResp)
				}
			}
		}
	}
}
func createTestServer() (*server.Server, *server.Config, error) {
	conf := server.DefaultConfig()
	conf.ListenAddr = "127.0.0.1:0"

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	serv, err := server.NewServer(conf, poolFactory)
	return serv, conf, err
}
