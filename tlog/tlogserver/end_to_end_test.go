package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/g8os/blockstor/tlog/tlogclient"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

func TestEndToEnd(t *testing.T) {
	// create server
	conf := &config{
		K:          4,
		M:          2,
		listenAddr: "127.0.0.1:0",
		flushSize:  25,
		flushTime:  25,
		privKey:    "12345678901234567890123456789012",
		nonce:      "37b8e8a308c354048d245f6d",
	}
	conf.initObjStoreAddress("")

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

	for i := 0; i < conf.flushSize*numFlush; i++ {
		x := uint64(i)
		// check we can send it without error
		err := client.Send(expectedVdiskID, x, x, x, data)
		assert.Nil(t, err)

		// check there is no error from server
		tr, err := client.RecvOne()
		assert.Nil(t, err)
		assert.Equal(t, true, tr.Status >= 0)
	}

	// decode the message
	dec, err := decoder.New(s.ObjStorAddresses, conf.K, conf.M, expectedVdiskID, conf.privKey, conf.nonce)
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
		assert.Equal(t, uint64(conf.flushSize), agg.Size())

		vdiskID, err := agg.VdiskID()
		assert.Nil(t, err)
		assert.Equal(t, expectedVdiskID, vdiskID)

		blocks, err := agg.Blocks()
		assert.Nil(t, err)
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
