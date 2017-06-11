package blockbuffer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/tlog/schema"
)

var (
	ErrRetryExceeded = errors.New("retry exceeded")
)

// Buffer defines buffer of tlog blocks that already sent
// but still waiting to be succesfully received by the server
type Buffer struct {
	lock            sync.RWMutex
	entries         map[uint64]*entry
	seqToTimeout    []uint64
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

// Len returns number of blocks in this buffer
func (b *Buffer) Len() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return len(b.entries)
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

	// add to array of ordered timeout
	b.seqToTimeout = append(b.seqToTimeout, seq)

	// only need to update the timeout
	// if already exist in the buffer
	ent, exist := b.entries[seq]
	if exist {
		ent.update(b.timeout, 1)
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

// Returns true this seq need to be re-send
func (b *Buffer) NeedResend(seq uint64) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	_, ok := b.entries[seq]
	return ok
}

// get one timed out block from the buffer
func (b *Buffer) getOne() *entry {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.seqToTimeout) == 0 {
		return nil
	}

	var i int
	var seq uint64

	defer func() {
		b.seqToTimeout = b.seqToTimeout[i+1:] // truncate processed blocks
	}()

	for i, seq = range b.seqToTimeout {
		// if not exist anymore in the map
		// it means the blocks already delivered
		if ent, ok := b.entries[seq]; ok {
			return ent
		}

		if i == 1000 { // be nice with others by not taking all the cpu
			return nil
		}

	}
	return nil
}

// TimedOut returns channel of timed out block
func (b *Buffer) TimedOut(ctx context.Context) <-chan *schema.TlogBlock {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ent := b.getOne()
				if ent == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// nanosecond left before timeout
				toTime := ent.timeout - time.Now().UnixNano()
				if toTime > 0 {
					time.Sleep(time.Duration(toTime) * time.Nanosecond)
				}

				b.readyChan <- ent.block
			}
		}
	}()

	return b.readyChan
}

// MinSequence returns min sequence that this
// buffer will/currently has.
func (b *Buffer) MinSequence() uint64 {
	b.lock.RLock()
	defer b.lock.RUnlock()

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
