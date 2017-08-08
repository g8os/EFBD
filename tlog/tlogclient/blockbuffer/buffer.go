package blockbuffer

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/tlog/schema"
)

var (
	ErrRetryExceeded = errors.New("retry exceeded")
)

// Buffer defines buffer of tlog blocks that already sent
// but still waiting for:
// - to be succesfully received by the server
// - to be flushed
type Buffer struct {
	lock sync.RWMutex

	// map of sent blocks.
	// blocks in this map is going to moved to
	// waitToFlush map after it receive sent confirmation
	// from the tlog server
	entries map[uint64]*entry

	// Array of ordered sent sequence.
	// It is used to find the timed out block
	seqToTimeout []uint64

	// map of entries that waiting to be flushed
	waitToFlush map[uint64]*entry

	// channel of blocks that ready to be re-sent
	readyChan chan *schema.TlogBlock

	// block timeout value. Block will be re-send
	// if reach this timeout value
	timeout time.Duration

	// the highest sequence we received
	highestSequence uint64

	// last flushed sequence
	lastFlush uint64
}

// NewBuffer creates a new tlog blocks buffer
func NewBuffer(timeout time.Duration) *Buffer {
	return &Buffer{
		readyChan:   make(chan *schema.TlogBlock, 1),
		entries:     make(map[uint64]*entry),
		waitToFlush: make(map[uint64]*entry),
		timeout:     timeout,
	}
}

// Uint64Slice implements sort interface for buffer.seqToTimeout
type Uint64Slice []uint64

func (us Uint64Slice) Len() int           { return len(us) }
func (us Uint64Slice) Swap(i, j int)      { us[i], us[j] = us[j], us[i] }
func (us Uint64Slice) Less(i, j int) bool { return us[i] < us[j] }

// SetResendAll resets this buffer, make all blocks
// need to be resend right now
func (b *Buffer) SetResendAll() {
	b.lock.Lock()
	defer b.lock.Unlock()

	// empty the seqToTimeout slice
	b.seqToTimeout = make([]uint64, 0, len(b.entries)+len(b.waitToFlush))

	for seq, ent := range b.entries {
		b.seqToTimeout = append(b.seqToTimeout, seq)
		ent.setZero()
	}

	for seq, ent := range b.waitToFlush {
		b.seqToTimeout = append(b.seqToTimeout, seq)
		ent.setZero()
		b.entries[seq] = ent
	}

	sort.Sort(Uint64Slice(b.seqToTimeout))

	b.waitToFlush = make(map[uint64]*entry)
}

// Add adds a block to this buffer.
// It only update the states if already exist.
func (b *Buffer) Add(block *schema.TlogBlock) {
	b.lock.Lock()
	defer b.lock.Unlock()

	seq := block.Sequence()

	if seq < b.lastFlush {
		return
	}

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

// SetFlushed set this sequence as succesfully flushed
func (b *Buffer) SetFlushed(seqs []uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(seqs) == 0 {
		return
	}

	for _, seq := range seqs {
		delete(b.waitToFlush, seq)
		delete(b.entries, seq)
	}

	// update lastFlush
	lastSeq := seqs[len(seqs)-1]
	if b.lastFlush < lastSeq {
		b.lastFlush = lastSeq
	}
}

// LastFlushed returns last sequence flushed
func (b *Buffer) LastFlushed() uint64 {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.lastFlush
}

// SetSent set this sequence as succesfully sent
func (b *Buffer) SetSent(seq uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	ent, ok := b.entries[seq]
	if !ok {
		return
	}
	b.waitToFlush[seq] = ent

	delete(b.entries, seq)
}

// NeedResend returns true if this sequence need to be re-send
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
				b.lock.RLock()
				toTime := ent.timeout - time.Now().UnixNano()
				block := ent.block
				b.lock.RUnlock()

				if toTime > 0 {
					time.Sleep(time.Duration(toTime) * time.Nanosecond)
				}

				b.readyChan <- block
			}
		}
	}()

	return b.readyChan
}
