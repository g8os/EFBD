package tlog

import (
	"bytes"
	"errors"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog/schema"
)

var (
	// ErrExceedBlockNumber returned in case user tries to
	// add block to a full aggregation
	ErrExceedBlockNumber = errors.New("exceed blocks number")

	// ErrEmptyAggregation returned in case the aggregation is empty
	// but user tries to do operation for non-empty aggregation
	ErrEmptyAggregation = errors.New("empty aggregation")
)

// Aggregation defines a tlog aggregation
type Aggregation struct {
	agg          schema.TlogAggregation
	msg          *capnp.Message
	blockList    schema.TlogBlock_List
	lastSequence uint64
	size         int
	maxBlockNum  int
}

// NewAggregation creates an aggregation with given capnp buffer
// and maximal block number
func NewAggregation(capnpBuf []byte, maxBlockNum int) (*Aggregation, error) {
	// create the aggregation object
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(capnpBuf))
	if err != nil {
		return nil, err
	}
	agg, err := schema.NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}
	blockList, err := agg.NewBlocks(int32(maxBlockNum))
	if err != nil {
		return nil, err
	}
	return &Aggregation{
		agg:         agg,
		msg:         msg,
		blockList:   blockList,
		maxBlockNum: maxBlockNum,
	}, nil
}

// Empty returns true if the aggregation is empty
func (a *Aggregation) Empty() bool {
	return a.size == 0
}

// Full returns true if the aggregation is full
func (a *Aggregation) Full() bool {
	return a.size == a.maxBlockNum
}

// LastSequence returns last sequence in this aggregation
func (a *Aggregation) LastSequence() uint64 {
	return a.lastSequence
}

// Sequences returns all sequences number in this aggregation
func (a *Aggregation) Sequences() []uint64 {
	if a.size == 0 {
		return nil
	}

	seqs := make([]uint64, a.size)
	for i := 0; i < a.size; i++ {
		seqs[i] = a.blockList.At(i).Sequence()
	}
	return seqs
}

// AddTransaction add a transaction to this aggregation
func (a *Aggregation) AddTransaction(t Transaction) error {
	if a.size == a.maxBlockNum {
		return ErrExceedBlockNumber
	}
	block := a.blockList.At(a.size)
	block.SetOperation(t.Operation)
	block.SetSequence(t.Sequence)
	if err := block.SetData(t.Content); err != nil {
		return err
	}
	if err := block.SetHash(t.Hash); err != nil {
		return err
	}
	block.SetIndex(t.Index)
	block.SetTimestamp(t.Timestamp)

	a.lastSequence = t.Sequence
	a.size++
	return nil
}

// AddBlock add a capnp block to this aggregation
func (a *Aggregation) AddBlock(block *schema.TlogBlock) error {
	if a.size == a.maxBlockNum {
		return ErrExceedBlockNumber
	}

	newBlock := a.blockList.At(a.size)
	if err := schema.CopyBlock(&newBlock, block); err != nil {
		return err
	}

	a.lastSequence = block.Sequence()
	a.size++
	return nil
}

// IgnoreSeqBefore ignore all blocks with sequence before the given
// sequence
func (a *Aggregation) IgnoreSeqBefore(seq uint64) error {
	// find last id to remove
	idx := -1
	for i := 0; i < a.size; i++ {
		if a.blockList.At(i).Sequence() >= seq {
			break
		}
		idx = i
	}
	if idx < 0 {
		return nil
	}
	a.size = a.size - (idx + 1)
	if a.size == 0 {
		return nil
	}

	// shift the blocks
	for i := 0; i < idx; i++ {
		block := a.blockList.At(i + idx + 1)
		if err := a.blockList.Set(i, block); err != nil {
			return err
		}
	}
	return nil
}

// SetTimestamp sets timestamp of this aggregation
func (a *Aggregation) SetTimestamp(timestamp int64) {
	a.agg.SetTimestamp(timestamp)
}

// Encode encodes this aggregation to byte slice
func (a *Aggregation) Encode() ([]byte, error) {
	a.agg.SetSize(uint64(a.size))
	buf := new(bytes.Buffer)
	err := capnp.NewEncoder(buf).Encode(a.msg)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
