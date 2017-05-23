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
}

func (bw bufWriter) Flush() error {
	return nil
}

type dummyServer struct {
	serv      *server.Server
	w         bufWriter
	respBuf   *bytes.Buffer
	readChan  chan struct{}
	writeChan chan struct{}
}

func newDummyServer(s *server.Server) *dummyServer {
	return &dummyServer{
		serv: s,
		w: bufWriter{
			Buffer: new(bytes.Buffer),
		},
		respBuf:   new(bytes.Buffer),
		readChan:  make(chan struct{}, 1),
		writeChan: make(chan struct{}, 1),
	}
}

func (ds *dummyServer) handle(t *testing.T) error {
	for {
		// receive the message
		<-ds.readChan
		block, err := ds.serv.ReadDecodeBlock(ds.w)
		if err != nil {
			t.Fatalf("error decode block:%v", err)
			continue
		}
		t.Logf("block sequence:%v", block.Sequence())

		// send resp
		resp := server.BlockResponse{
			Status:    int8(tlog.BlockStatusRecvOK),
			Sequences: []uint64{block.Sequence()},
		}
		resp.Write(ds.respBuf, nil)
		ds.writeChan <- struct{}{}
	}
}

func TestResend(t *testing.T) {
	const (
		vdisk         = "12345"
		firstSequence = 0
	)

	// only used in connect
	// TODO : remove the need to this unused server
	unusedServer, err := createUnusedServer()
	assert.Nil(t, err)
	go unusedServer.Listen()
	ds := newDummyServer(unusedServer)
	go ds.handle(t)

	client, err := New(unusedServer.ListenAddr(), vdisk, firstSequence)
	//err = client.init(firstSequence)
	assert.Nil(t, err)

	//
	data := make([]byte, 4096)
	// fake the connection
	client.bw = ds.w

	numLogs := 24

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		client.rd = ds.respBuf

		for i := 0; i < numLogs; i++ {
			x := uint64(i)
			// recv
			<-ds.writeChan
			tr, err := client.recvOne()
			assert.Nil(t, err)

			// check response content
			assert.Equal(t, tr.Status, tlog.BlockStatusRecvOK)
			assert.Equal(t, 1, len(tr.Sequences))
			seqResp := tr.Sequences[0]
			assert.Equal(t, x, seqResp)
		}
	}()

	for i := 0; i < numLogs; i++ {
		x := uint64(i)

		// send
		err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
		assert.Nil(t, err)
		ds.readChan <- struct{}{}
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
