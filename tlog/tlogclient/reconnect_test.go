package tlogclient

import (
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
		numFlush      = 1
	)

	serv, servConf, err := createTestServer()
	assert.Nil(t, err)
	go serv.Listen()

	client, err := New(serv.ListenAddr(), vdisk, firstSequence)
	assert.Nil(t, err)

	numLogs := servConf.FlushSize - 1

	data := make([]byte, 4096)
	oneTenth := numLogs / 10

	// send tlog, and disconnect it few times in the middle to test re-connect ability.
	// to not create unnecessary complication, we do these:
	// - we use sync funcs
	// - send below flushSize
	for i := 0; i < numLogs; i++ {
		x := uint64(i)

		if numLogs%oneTenth == 0 { // close the connection every one-tenth of numLogs
			client.conn.Close() // simulate closed connection by closing the socket
		}

		// send
		err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
		assert.Nil(t, err)

		// recv
		tr, err := client.recvOne()
		assert.Nil(t, err)

		// check response content
		assert.Equal(t, tr.Status, tlog.BlockStatusRecvOK)
		assert.Equal(t, 1, len(tr.Sequences))
		seqResp := tr.Sequences[0]
		assert.Equal(t, x, seqResp)
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
