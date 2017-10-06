package tlogclient

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// implements the writerFlusher
type pipeWriterFlush struct {
	*io.PipeWriter
}

func (pwf pipeWriterFlush) Flush() error {
	return nil
}

// implements pipe read with timeout
type pipeReaderTimeout struct {
	*io.PipeReader
}

// Read implements read with timeout
func (prt pipeReaderTimeout) Read(data []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	resultCh := make(chan result, 1)

	go func() {
		n, err := prt.PipeReader.Read(data)
		resultCh <- result{
			n:   n,
			err: err,
		}
	}()
	select {
	case <-time.After(readTimeout):
		return 0, io.EOF
	case res := <-resultCh:
		return res.n, res.err
	}
}

// dummy tlog server that only receive request
// and give response.
// It use io.Pipe to simulate TCP connection.
type dummyServer struct {
	serv          *server.Server
	reqPipeWriter pipeWriterFlush
	reqPipeReader *io.PipeReader

	respPipeWriter *io.PipeWriter
	respPipeReader pipeReaderTimeout
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
		respPipeReader: pipeReaderTimeout{
			PipeReader: respRd,
		},
	}
}

func readDecodeClientMessage(r io.Reader) (*schema.TlogClientMessage, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}
	cmd, err := schema.ReadRootTlogClientMessage(msg)
	if err != nil {
		return nil, err
	}
	return &cmd, nil
}

// run this dummy server.
func (ds *dummyServer) run(t *testing.T, logsToIgnore map[uint64]struct{}) error {
	for {
		// read client command
		cmd, err := readDecodeClientMessage(ds.reqPipeReader)
		if err != nil {
			t.Fatal("failed to read client message")
		}

		if w := cmd.Which(); w != schema.TlogClientMessage_Which_block {
			t.Fatalf("unhandled client message: %v", w)
		}

		// get block
		block, err := cmd.Block()
		if err != nil {
			t.Fatalf("error getting block from block client message: %v", err)
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
// it simulates timeout by create dummy server that
// ignore some of the logs
func TestResendTimeout(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk         = "12345"
		firstSequence = 0
		numLogs       = 500
	)

	clean, configSource, _ := newZeroStorDefaultConfig(t, vdisk)
	defer clean()
	// only used in client.connect
	// TODO : remove the need to this unused server
	unusedServer, err := server.NewServer(testConf, configSource)
	assert.Nil(t, err)
	go unusedServer.Listen(ctx)

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

	client, _, err := newClient([]string{unusedServer.ListenAddr()}, vdisk)
	assert.Nil(t, err)
	defer client.Close()

	data := make([]byte, 4096)

	client.bw = ds.reqPipeWriter  // fake client writer
	client.rd = ds.respPipeReader // fake the reader

	go client.run(client.ctx)

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
		err := client.Send(schema.OpSet, x, int64(x), int64(x), data)
		assert.Nil(t, err)
	}

	wg.Wait()
}
