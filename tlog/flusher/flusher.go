package flusher

import (
	"sync"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

type transaction struct {
	Operation uint8
	Sequence  uint64
	Content   []byte
	Index     int64
	Timestamp int64
}

// Flusher defines a tlog flusher
type Flusher struct {
	mux          sync.Mutex
	flushSize    int
	storCli      *stor.Client
	transactions []transaction
}

// New creates a new flusher
func New(confSource config.Source, dataShards, parityShards int, vdiskID, privKey string) (*Flusher, error) {
	// creates stor client
	storConf, err := stor.ConfigFromConfigSource(confSource, vdiskID, privKey, dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	storCli, err := stor.NewClient(storConf)
	if err != nil {
		return nil, err
	}

	tlogConf := server.DefaultConfig()

	return &Flusher{
		storCli:   storCli,
		flushSize: tlogConf.FlushSize,
	}, nil
}

// AddTransaction add transaction to this flusher and
// flush it if it reach FlushSize
func (f *Flusher) AddTransaction(op uint8, seq uint64, content []byte, index, timestamp int64) error {
	f.mux.Lock()
	defer f.mux.Unlock()

	f.transactions = append(f.transactions, transaction{
		Operation: op,
		Sequence:  seq,
		Content:   content,
		Index:     index,
		Timestamp: timestamp,
	})
	if len(f.transactions) < f.flushSize {
		return nil
	}
	_, err := f.flush()
	return err
}

// Flush flushes the transactions in buffer
func (f *Flusher) Flush() (int, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	n, err := f.flush()
	return n, err
}

func (f *Flusher) flush() (flushed int, err error) {
	if len(f.transactions) == 0 {
		return
	}

	// count flush size
	flushSize := f.flushSize
	if len(f.transactions) < flushSize {
		flushSize = len(f.transactions)
	}

	// take transactions to flush
	toFlush := f.transactions[:flushSize]
	f.transactions = f.transactions[flushSize:]

	blocks := make([]*schema.TlogBlock, 0, len(toFlush))
	for _, t := range toFlush {
		var block *schema.TlogBlock
		block, err = f.transactionToBlock(t)
		if err != nil {
			f.transactions = append(toFlush, f.transactions...)
			return
		}
		blocks = append(blocks, block)
	}

	_, err = f.storCli.ProcessStore(blocks)
	flushed = flushSize

	return
}

func (f *Flusher) transactionToBlock(t transaction) (*schema.TlogBlock, error) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	block, err := schema.NewTlogBlock(seg)
	if err != nil {
		return nil, err
	}

	hash := zerodisk.Hash(t.Content)
	if err := block.SetHash(hash); err != nil {
		return nil, err
	}

	if err := block.SetData(t.Content); err != nil {
		return nil, err
	}

	block.SetOperation(t.Operation)
	block.SetSequence(t.Sequence)
	block.SetIndex(t.Index)
	block.SetTimestamp(t.Timestamp)

	return &block, nil
}
