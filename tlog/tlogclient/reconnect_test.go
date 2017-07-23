package tlogclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

// TestReconnectFromSend test client can connect again after getting disconnected
// when doing 'Send'
func TestReconnectFromSend(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk         = "12345"
		firstSequence = 0
		numLogs       = 50
	)

	serv, _, err := createTestServer()
	assert.Nil(t, err)
	go serv.Listen(ctx)

	client, err := New([]string{serv.ListenAddr()}, vdisk, firstSequence, false)
	assert.Nil(t, err)
	defer client.Close()

	data := make([]byte, 4096)

	// send tlog, and disconnect it few times in the middle to test re-connect ability.
	// don't start Recv so we don't reconnect from the receiver
	for i := 0; i < numLogs; i++ {
		x := uint64(i)
		if i%5 == 0 {
			client.conn.Close() // simulate closed connection by closing the socket
		}

		// send
		err := client.Send(schema.OpSet, x, int64(x), x, data)
		assert.Nil(t, err)
	}

	waitForBlockReceivedResponse(t, client, 0, uint64(numLogs)-1)
}

// TestReconnectFromRead test that client can do reconnect from 'Recv'
// scenarios:
// (1) create client
// (2) close connection
// (3) start the receiver -> the reconnect is going to happen here
// (4) Send ForceFlush using private API, so it doesn't have reconnect logic
// (5) wait for the confirmation, that force flush arrived
func TestReconnectFromRead(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk = "12345"
	)
	// test server
	s, _, err := createTestServer()
	assert.Nil(t, err)
	go s.Listen(ctx)

	//readTimeout = 10 * time.Millisecond
	// Step #1
	client, err := New([]string{s.ListenAddr()}, vdisk, 0, false)
	assert.Nil(t, err)

	// Step #2
	client.conn.Close()

	// Step #3
	respCh := client.Recv()
	time.Sleep(2 * readTimeout) // wait for the re-connect

	// Step #4
	client.wLock.Lock()
	err = client.forceFlushAtSeq(uint64(1))
	client.wLock.Unlock()
	assert.Nil(t, err)

	// Step #5
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("TestReconnectFromRead failed : too long")
	case resp := <-respCh:
		assert.Nil(t, resp.Err)
		assert.NotNil(t, resp.Resp)
		assert.Equal(t, resp.Resp.Status, tlog.BlockStatusForceFlushReceived)
	}
}

func (c *Client) forceFlushAtSeq(seq uint64) error {
	if err := tlog.WriteMessageType(c.bw, tlog.MessageForceFlushAtSeq); err != nil {
		return err
	}
	if err := c.encodeSendCommand(c.bw, tlog.MessageForceFlushAtSeq, seq); err != nil {
		return err
	}
	return c.bw.Flush()
}

func TestReconnectFromForceFlush(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk = "12345"
	)
	// test server
	s, _, err := createTestServer()
	assert.Nil(t, err)
	go s.Listen(ctx)

	// Create client
	client, err := New([]string{s.ListenAddr()}, vdisk, 0, false)
	assert.Nil(t, err)

	// Simulate closed connection
	client.conn.Close()

	// Do forceFlush, it should reconnect here
	err = client.ForceFlushAtSeq(uint64(0))
	assert.Nil(t, err)

	respCh := client.Recv()

	// Wait for the response
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("TestReconnectFromRead failed : too long")
	case resp := <-respCh:
		assert.Nil(t, resp.Err)
		assert.NotNil(t, resp.Resp)
		assert.Equal(t, resp.Resp.Status, tlog.BlockStatusForceFlushReceived)
	}
}

func waitForBlockReceivedResponse(t *testing.T, client *Client, minSequence, maxSequence uint64) {
	// map of sequence we want to wait for the response to come
	logsToRecv := map[uint64]struct{}{}
	for i := minSequence; i <= maxSequence; i++ {
		logsToRecv[i] = struct{}{}
	}

	respChan := client.Recv()

	for len(logsToRecv) > 0 {
		// recv
		resp := <-respChan

		if resp.Err == nil {
			// check response content
			response := resp.Resp
			if response == nil {
				continue
			}
			switch response.Status {
			case tlog.BlockStatusRecvOK:
				assert.Equal(t, 1, len(response.Sequences))
				delete(logsToRecv, response.Sequences[0])
			case tlog.BlockStatusFlushOK: // if flushed, it means all previous already received
				maxSeq := response.Sequences[len(response.Sequences)-1]
				var seq uint64
				for seq = 0; seq <= maxSeq; seq++ {
					delete(logsToRecv, seq)
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
