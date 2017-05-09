package server

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

func TestEndToEnd(t *testing.T) {
	// create server
	conf := &Config{
		K:          4,
		M:          2,
		ListenAddr: "127.0.0.1:0",
		FlushSize:  25,
		FlushTime:  25,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}
	err := conf.ValidateAndCreateObjStoreAddresses(true)
	assert.Nil(t, err)

	s, err := NewServer(conf)
	assert.Nil(t, err)

	go func() {
		s.Listen()
	}()

	// send tlog messages
	client, err := tlogclient.New(s.ListenAddr())
	assert.Nil(t, err)

	t.Logf("listen addr=%v", s.ListenAddr())

	dataLen := 4096 * 4

	data := make([]byte, dataLen)
	for i := 0; i < (dataLen); i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	expectedVdiskID := "1234567890"
	numFlush := 5

	var wg sync.WaitGroup

	wg.Add(2)

	numLogs := conf.FlushSize * numFlush // number of logs to send.

	// send tlog
	go func() {
		defer wg.Done()
		for i := 0; i < numLogs; i++ {
			x := uint64(i)
			// check we can send it without error
			err := client.Send(expectedVdiskID, schema.OpWrite, x, x, x, data, uint64(len(data)))
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
			assert.Nil(t, re.Err)
			assert.Equal(t, true, re.Resp.Status >= 0)
		}
	}()

	wg.Wait()

	// decode the message
	dec, err := decoder.New(s.ObjStorAddresses, conf.K, conf.M, expectedVdiskID, conf.PrivKey, conf.HexNonce)
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

			// check vdisk id
			vdiskID, err := block.VdiskID()
			assert.Nil(t, err)
			assert.Equal(t, expectedVdiskID, vdiskID)
		}

		aggReceived++
	}
	assert.Equal(t, numFlush, aggReceived)
}
