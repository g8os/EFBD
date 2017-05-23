package tlogclient

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogserver/server"
)

type bufWriter struct {
	*bytes.Buffer
	notifChan chan struct{}
}

func (bw bufWriter) Flush() error {
	bw.notifChan <- struct{}{}
	return nil
}

type dummyServer struct {
	serv         *server.Server
	reqBuf       bufWriter     // request buffer
	reqNotifChan chan struct{} // request availability notification channel

	respBuf       *bytes.Buffer // resp buffer
	respNotifChan chan struct{} // resp availability notification channel
}

func newDummyServer(s *server.Server) *dummyServer {
	reqNotifChan := make(chan struct{}, 1)
	respNotifChan := make(chan struct{}, 1)
	return &dummyServer{
		serv: s,
		reqBuf: bufWriter{
			Buffer:    new(bytes.Buffer),
			notifChan: reqNotifChan,
		},
		respBuf:       new(bytes.Buffer),
		reqNotifChan:  reqNotifChan,
		respNotifChan: respNotifChan,
	}
}

func (ds *dummyServer) handle(t *testing.T, logsToIgnore map[uint64]struct{}) error {
	for {
		// receive the message
		<-ds.reqNotifChan
		block, err := ds.serv.ReadDecodeBlock(ds.reqBuf)
		if err != nil {
			t.Fatalf("error decode block:%v", err)
			continue
		}
		seq := block.Sequence()

		if _, ok := logsToIgnore[seq]; ok {
			delete(logsToIgnore, seq)
			continue
		}

		// send resp
		resp := server.BlockResponse{
			Status:    int8(tlog.BlockStatusRecvOK),
			Sequences: []uint64{block.Sequence()},
		}
		resp.Write(ds.respBuf, nil)
		ds.respNotifChan <- struct{}{}
	}
}

func TestResend(t *testing.T) {
	const (
		vdisk         = "12345"
		firstSequence = 0
		numLogs       = 500
	)

	// only used in connect
	// TODO : remove the need to this unused server
	unusedServer, err := createUnusedServer()
	assert.Nil(t, err)
	go unusedServer.Listen()

	// list of sequences for the server to ignores.
	// it simulates timeout
	logsToIgnore := map[uint64]struct{}{}
	for i := 0; i < numLogs; i++ {
		if i%5 == 0 {
			logsToIgnore[uint64(i)] = struct{}{}
		}
	}

	ds := newDummyServer(unusedServer)
	go ds.handle(t, logsToIgnore)

	client, err := New(unusedServer.ListenAddr(), vdisk, firstSequence)
	assert.Nil(t, err)

	data := make([]byte, 4096)

	// fake client writer to server's request buffer
	client.bw = ds.reqBuf

	var wg sync.WaitGroup

	// start receiver goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.rd = ds.respBuf // fake the reader

		// map of sequence we want to wait for the response to come
		logsToRecv := map[uint64]struct{}{}
		for i := 0; i < numLogs; i++ {
			logsToRecv[uint64(i)] = struct{}{}
		}

		<-ds.respNotifChan
		respChan := client.Recv(0) // make it blocking by setting channel size to 0

		// TODO: because of the limitation of this test (need ds.respNotifChan)
		// there will be always one message we can't wait
		for len(logsToRecv) > 1 {
			// recv
			<-ds.respNotifChan
			resp := <-respChan

			assert.Nil(t, resp.Err)

			// check response content
			if resp != nil && resp.Resp.Status == tlog.BlockStatusRecvOK {
				assert.Equal(t, 1, len(resp.Resp.Sequences))
				seqResp := resp.Resp.Sequences[0]

				if _, ok := logsToRecv[seqResp]; ok {
					delete(logsToRecv, seqResp)
				}
			}

		}
	}()

	// send the logs
	for i := 0; i < numLogs; i++ {
		x := uint64(i)

		// send
		err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
		assert.Nil(t, err)
	}

	wg.Wait()
}

func createUnusedServer() (*server.Server, error) {
	conf := server.DefaultConfig()
	conf.ListenAddr = "127.0.0.1:0"

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	return server.NewServer(conf, poolFactory)
}
