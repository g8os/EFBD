package flusher

import (
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
)

const (
	// DefaultFlushSize is default flush size value
	DefaultFlushSize = 25
)

// Flusher defines a tlog flusher
type Flusher struct {
	mux       sync.Mutex
	flushSize int
	storCli   *stor.Client
	curAgg    *tlog.Aggregation
	capnpBuf  []byte
}

// New creates a new flusher
func New(confSource config.Source, flushSize int,
	vdiskID, privKey string) (*Flusher, error) {
	// creates stor client
	storConf, err := stor.ConfigFromConfigSource(confSource, vdiskID, privKey)
	if err != nil {
		return nil, err
	}
	storCli, err := stor.NewClient(storConf)
	if err != nil {
		return nil, err
	}

	// read vdisk conf
	vdiskConf, err := config.ReadVdiskStaticConfig(confSource, vdiskID)
	if err != nil {
		return nil, err
	}

	return NewWithStorClient(storCli, flushSize, int(vdiskConf.BlockSize)), nil
}

// NewWithStorClient creates new flusher with given stor.Client
func NewWithStorClient(storCli *stor.Client, flushSize, blockSize int) *Flusher {
	if flushSize == 0 {
		flushSize = DefaultFlushSize
	}

	return &Flusher{
		storCli:   storCli,
		flushSize: flushSize,
		capnpBuf:  make([]byte, 0, blockSize*(flushSize+1)),
	}
}

// AddTransaction add transaction to this flusher
func (f *Flusher) AddTransaction(t tlog.Transaction) error {
	f.mux.Lock()
	defer f.mux.Unlock()

	// creates aggregation if needed
	if f.curAgg == nil {
		if err := f.initAggregation(); err != nil {
			return err
		}
	}

	return f.curAgg.AddTransaction(t)
}

// AddBlock adds capnp block to this flusher
func (f *Flusher) AddBlock(block *schema.TlogBlock) error {
	f.mux.Lock()
	defer f.mux.Unlock()

	// creates aggregation if needed
	if f.curAgg == nil {
		if err := f.initAggregation(); err != nil {
			return err
		}
	}

	return f.curAgg.AddBlock(block)
}

func (f *Flusher) initAggregation() error {
	agg, err := tlog.NewAggregation(f.capnpBuf, f.flushSize)
	if err != nil {
		return err
	}
	f.curAgg = agg
	return nil
}

// Full returns true if the flusher's aggregation is full
func (f *Flusher) Full() bool {
	if f.curAgg == nil {
		return false
	}
	return f.curAgg.Full()
}

// Empty returns true if the flusher's aggregation is empty
func (f *Flusher) Empty() bool {
	if f.curAgg == nil {
		return true
	}
	return f.curAgg.Empty()
}

// IgnoreSeqBefore ignores all blocks with sequence less than
// the given sequence
func (f *Flusher) IgnoreSeqBefore(seq uint64) error {
	if f.curAgg == nil {
		return nil
	}
	return f.curAgg.IgnoreSeqBefore(seq)
}

// Flush flushes the transactions in buffer
func (f *Flusher) Flush() ([]byte, []uint64, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	return f.flush()
}

func (f *Flusher) flush() ([]byte, []uint64, error) {
	if f.curAgg == nil || f.curAgg.Empty() {
		return nil, nil, nil
	}

	data, err := f.storCli.ProcessStoreAgg(f.curAgg)
	if err != nil {
		return nil, nil, err
	}

	seqs := f.curAgg.Sequences()

	f.curAgg = nil

	return data, seqs, nil
}
