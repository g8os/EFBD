package tlogclient

import (
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogserver/server"
)

// implements the writerFlusher
type pipeWriterFlush struct {
	*io.PipeWriter
}

func (pwf pipeWriterFlush) Flush() error {
	return nil
}

// dummy tlog server that only receive request
// and give response.
// It use io.Pipe to simulate TCP connection.
type dummyServer struct {
	serv          *server.Server
	reqPipeWriter pipeWriterFlush
	reqPipeReader *io.PipeReader

	respPipeWriter *io.PipeWriter
	respPipeReader *io.PipeReader
}

func newDummyServer(s *server.Server) *dummyServer {
	reqRd, reqW := io.Pipe()
	respRd, respW := io.Pipe()
	return &dummyServer{
		serv: s,
		reqPipeWriter: pipeWriterFlush{
			PipeWriter: reqW,
		},
		reqPipeReader:  reqRd,
		respPipeWriter: respW,
		respPipeReader: respRd,
	}
}

// run this dummy server.
func (ds *dummyServer) run(t *testing.T, logsToIgnore map[uint64]struct{}) error {
	for {
		// receive the message
		block, err := ds.serv.ReadDecodeBlock(ds.reqPipeReader)
		if err != nil {
			t.Fatalf("error decode block:%v", err)
			continue
		}
		seq := block.Sequence()

		// ignore this log, if it exist in logsToIgnore map
		if _, ok := logsToIgnore[seq]; ok {
			delete(logsToIgnore, seq)
			continue
		}

		// send resp
		resp := server.BlockResponse{
			Status:    int8(tlog.BlockStatusRecvOK),
			Sequences: []uint64{block.Sequence()},
		}
		resp.Write(ds.respPipeWriter, nil)
	}
}

// TestResendTimeout test client resend in case of timeout
func TestResendTimeout(t *testing.T) {
	const (
		vdisk         = "12345"
		firstSequence = 0
		numLogs       = 500
	)

	// only used in client.connect
	// TODO : remove the need to this unused server
	unusedServer, _, err := createTestServer()
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
	go ds.run(t, logsToIgnore)

	client, err := New(unusedServer.ListenAddr(), vdisk, firstSequence)
	assert.Nil(t, err)

	data := make([]byte, 4096)

	client.bw = ds.reqPipeWriter  // fake client writer
	client.rd = ds.respPipeReader // fake the reader

	var wg sync.WaitGroup

	// start receiver goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitForBlockReceivedResponse(t, client, 0, numLogs-1)
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
