package server

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

var (
	testConf = &Config{
		K:          4,
		M:          2,
		ListenAddr: "127.0.0.1:0",
		FlushSize:  25,
		FlushTime:  10,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}
)

// Test that we can send the data to tlog and decode it again correctly
func TestEndToEnd(t *testing.T) {
	// config
	conf := testConf

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	s, err := NewServer(conf, poolFactory)
	assert.Nil(t, err)

	go s.Listen()
	t.Logf("listen addr=%v", s.ListenAddr())

	const (
		expectedVdiskID = "1234567890"
		firstSequence   = 0
	)

	// create tlog client
	client, err := tlogclient.New(s.ListenAddr(), expectedVdiskID, firstSequence)
	if !assert.Nil(t, err) {
		return
	}

	// initialize test data
	dataLen := 4096 * 4

	data := make([]byte, dataLen)
	for i := 0; i < (dataLen); i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	const numFlush = 5

	var wg sync.WaitGroup

	wg.Add(2)

	numLogs := conf.FlushSize * numFlush // number of logs to send.

	// send tlog
	go func() {
		defer wg.Done()
		for i := 0; i < numLogs; i++ {
			x := uint64(i)
			// check we can send it without error
			err := client.Send(schema.OpWrite, x, x, x, data, uint64(len(data)))
			assert.Nil(t, err)
		}
	}()

	// recv it
	go func() {
		defer wg.Done()
		expected := numLogs + numFlush
		received := 0
		respChan := client.Recv(1)
		for received < expected {
			re := <-respChan
			received++
			if !assert.Nil(t, re.Err) {
				continue
			}
			assert.Equal(t, true, re.Resp.Status > 0)

			respStatus := tlog.BlockStatus(re.Resp.Status)
			if respStatus == tlog.BlockStatusFlushOK {
				assert.Equal(t, conf.FlushSize, len(re.Resp.Sequences))
			} else if respStatus == tlog.BlockStatusRecvOK {
				assert.Equal(t, 1, len(re.Resp.Sequences))
			}
		}
	}()

	wg.Wait()

	// get the redis pool for the vdisk
	pool, err := s.poolFactory.NewRedisPool(expectedVdiskID)
	if !assert.Nil(t, err) {
		return
	}

	// decode the message
	dec, err := decoder.New(
		pool, conf.K, conf.M,
		expectedVdiskID, conf.PrivKey, conf.HexNonce)
	assert.Nil(t, err)

	aggChan := dec.Decode(0)

	aggReceived := 0
	for {
		da, more := <-aggChan
		if !more {
			break
		}
		assert.Nil(t, da.Err)

		agg := da.Agg
		assert.Equal(t, uint64(conf.FlushSize), agg.Size())

		vdiskID, err := agg.VdiskID()
		assert.Nil(t, err)
		assert.Equal(t, expectedVdiskID, vdiskID)

		blocks, err := agg.Blocks()
		assert.Nil(t, err)

		assert.Equal(t, conf.FlushSize, blocks.Len())
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)

			// check the data content
			blockData, err := block.Data()
			assert.Nil(t, err)
			assert.Equal(t, data, blockData)
		}

		aggReceived++
	}
	assert.Equal(t, numFlush, aggReceived)
}

// Test tlog server ability to handle unordered message
func TestUnordered(t *testing.T) {
	// config
	conf := testConf

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	// start the server
	s, err := NewServer(conf, poolFactory)
	assert.Nil(t, err)

	go s.Listen()

	t.Logf("listen addr=%v", s.ListenAddr())
	const (
		vdiskID       = "12345"
		firstSequence = 10
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

	const numFlush = 4
	numLogs := conf.FlushSize * numFlush // number of logs to send.
	seqs := []uint64{}
	for i := 0; i < numLogs; i++ {
		seqs = append(seqs, uint64(i)+firstSequence)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	// send tlog
	go func() {
		defer wg.Done()

		var seqIdx int
		for i := 0; i < numLogs; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// pick random sequence
				seqIdx = rand.Intn(len(seqs))

				seq := seqs[seqIdx]
				seqs = append(seqs[:seqIdx], seqs[seqIdx+1:]...)

				x := uint64(i)
				// check we can send it without error
				err := client.Send(schema.OpWrite, seq, x, x, data, uint64(len(data)))
				if !assert.Nil(t, err) {
					cancelFunc()
					return
				}

				// send it twice, to test duplicated message
				err = client.Send(schema.OpWrite, seq, x, x, data, uint64(len(data)))
				if !assert.Nil(t, err) {
					cancelFunc()
					return
				}
			}
		}
	}()

	expected := (numLogs * 2) + numFlush // multiply by 2 because we send duplicated message
	received := 0

	// recv it
	go func() {
		defer wg.Done()

		respChan := client.Recv(1)
		for received < expected {
			select {
			case re := <-respChan:
				received++
				if !assert.Nil(t, re.Err) {
					cancelFunc()
					return
				}
				if !assert.Equal(t, true, re.Resp.Status > 0) {
					cancelFunc()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	if !assert.Equal(t, expected, received) {
		return
	}

	// get the redis pool for the vdisk
	pool, err := s.poolFactory.NewRedisPool(vdiskID)
	if !assert.Nil(t, err) {
		return
	}

	// decode the message
	dec, err := decoder.New(
		pool, conf.K, conf.M,
		vdiskID, conf.PrivKey, conf.HexNonce)
	assert.Nil(t, err)

	aggChan := dec.Decode(0)

	var expectedSequence = uint64(firstSequence)
	for {
		da, more := <-aggChan
		if !more {
			break
		}
		assert.Nil(t, da.Err)

		agg := da.Agg

		blocks, err := agg.Blocks()
		assert.Nil(t, err)

		assert.Equal(t, conf.FlushSize, blocks.Len())
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			assert.Equal(t, expectedSequence, block.Sequence())
			expectedSequence++
		}
	}
}
