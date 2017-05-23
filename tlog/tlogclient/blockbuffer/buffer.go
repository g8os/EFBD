package blockbuffer

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/g8os/blockstor/tlog/schema"
)

var (
	ErrRetryExceeded = errors.New("retry exceeded")
)

// Buffer defines buffer of tlog blocks that already sent
// but still waiting to be succesfully received by the server
type Buffer struct {
	lock            sync.Mutex
	entries         map[uint64]*entry
	readyChan       chan *schema.TlogBlock
	timeout         time.Duration
	maxRetry        int
	highestSequence uint64 // the highest sequence we received
}

// NewBuffer creates a new tlog blocks buffer
func NewBuffer(timeout time.Duration) *Buffer {
	return &Buffer{
		readyChan: make(chan *schema.TlogBlock, 1),
		entries:   map[uint64]*entry{},
		timeout:   timeout,
		maxRetry:  3,
	}
}

// Promote sets a block with given sequence to be timed out now.
// It returns error if the block already exceed it's retry quota.
func (b *Buffer) Promote(seq uint64) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	ent, exist := b.entries[seq]
	if !exist {
		return fmt.Errorf("tlog client blockbuffer: seq %v not exist", seq)
	}

	if ent.retryNum >= b.maxRetry {
		return ErrRetryExceeded
	}

	ent.setTimeout()
	return nil
}

// Add adds a block to this buffer.
// It only update the states if already exist.
func (b *Buffer) Add(block *schema.TlogBlock) {
	b.lock.Lock()
	defer b.lock.Unlock()

	seq := block.Sequence()

	ent, exist := b.entries[seq]
	if exist {
		return
	}

	if seq > b.highestSequence {
		b.highestSequence = seq
	}

	ent = newEntry(block, b.timeout)

	b.entries[seq] = ent
}

// Delete deletes an entry from buffer.
func (b *Buffer) Delete(seq uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	delete(b.entries, seq)
}

// get all timed out entries.
// we need to optimize it.
func (b *Buffer) getTimedOut() []*entry {
	b.lock.Lock()
	defer b.lock.Unlock()

	now := time.Now()
	ents := []*entry{}
	for _, ent := range b.entries {
		if ent.isTimeout(now) {
			ents = append(ents, ent)
			ent.update(b.timeout)
		}
	}
	return ents
}

// TimedOut returns channel of timed out block
func (b *Buffer) TimedOut() <-chan *schema.TlogBlock {
	go func() {
		for {
			time.Sleep(time.Second)
			ents := b.getTimedOut()
			for _, ent := range ents {
				b.readyChan <- ent.block
			}
		}
	}()

	return b.readyChan
}

// MinSequence returns min sequence that this
// buffer will/currently has.
func (b *Buffer) MinSequence() uint64 {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.entries) == 0 {
		return b.highestSequence + 1
	}

	var seq uint64 = math.MaxUint64
	var blockSeq uint64
	for _, ent := range b.entries {
		blockSeq = ent.block.Sequence()
		if blockSeq < seq {
			seq = blockSeq
		}
	}
	return seq
}
