package server

import (
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/treeset"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

const (
	respChanSize = 10

	// tlogblock buffer size = flusher.flushSize * tlogBlockFactorSize
	// With buffer size that bigger than flushSize:
	// - we don't always block when flushing
	// - our RAM won't exploded because we still have upper limit
	tlogBlockFactorSize = 5
)

type vdisk struct {
	vdiskID          string
	lastHash         []byte                 // current in memory last hash
	lastHashKey      []byte                 // redis key of the last hash
	blockInputChan   chan *schema.TlogBlock // input channel of block received from client
	orderedBlockChan chan *schema.TlogBlock // ordered blocks from blockInputChan
	cmdChan          chan uint8             // channel of command
	respChan         chan *BlockResponse    // channel of responses to be sent to client
	flusher          *flusher
	segmentBuf       []byte // capnp segment buffer used by the flusher
	expectedSequence uint64 // expected sequence to be received
	lastSeqFlushed   uint64 // last sequence flushed
}

// ID returns the ID of this vdisk
func (vd *vdisk) ID() string {
	return vd.vdiskID
}

// ResponseChan returns the channel to which Block Responses get sent
func (vd *vdisk) ResponseChan() <-chan *BlockResponse {
	return vd.respChan
}

// creates vdisk with given vdiskID, flusher, and first sequence.
// firstSequence is the very first sequence that this vdisk will receive.
// blocks with sequence < firstSequence are going to be ignored.
func newVdisk(vdiskID string, f *flusher, firstSequence uint64, segmentBufLen int) (*vdisk, error) {
	// get last hash from storage
	lastHash, err := f.getLastHash(vdiskID)
	if err != nil {
		return nil, err
	}
	maxTlbInBuffer := f.flushSize * tlogBlockFactorSize

	return &vdisk{
		vdiskID:          vdiskID,
		lastHashKey:      decoder.GetLashHashKey(vdiskID),
		lastHash:         lastHash,
		blockInputChan:   make(chan *schema.TlogBlock, maxTlbInBuffer),
		orderedBlockChan: make(chan *schema.TlogBlock, maxTlbInBuffer),
		cmdChan:          make(chan uint8, 3),
		respChan:         make(chan *BlockResponse, respChanSize),
		expectedSequence: firstSequence,
		segmentBuf:       make([]byte, 0, segmentBufLen),
		flusher:          f,
	}, nil
}

type vdiskManager struct {
	vdisks           map[string]*vdisk
	lock             sync.Mutex
	maxSegmentBufLen int // max len of capnp buffer used by flushing process
}

func newVdiskManager(blockSize, flushSize int) *vdiskManager {
	// the estimation of max segment buf len we will need.
	// we add it by '1' because:
	// - the block will also container other data like 'sequenece', 'timestamp', etc..
	// - overhead of capnp schema
	segmentBufLen := blockSize * (flushSize + 1)

	return &vdiskManager{
		vdisks:           map[string]*vdisk{},
		maxSegmentBufLen: segmentBufLen,
	}
}

type flusherFactory func(vdiskID string) (*flusher, error)

// get or create the vdisk
func (vt *vdiskManager) Get(vdiskID string, firstSequence uint64, ff flusherFactory) (vd *vdisk, err error) {
	vt.lock.Lock()
	defer vt.lock.Unlock()

	vd, ok := vt.vdisks[vdiskID]
	if ok {
		return
	}

	f, err := ff(vdiskID)
	if err != nil {
		return
	}

	vd, err = newVdisk(vdiskID, f, firstSequence, vt.maxSegmentBufLen)
	if err != nil {
		return
	}
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	go vd.runFlusher()
	go vd.runReceiver()

	return
}

// the comparator function needed by https://godoc.org/github.com/emirpasic/gods/sets/treeset#NewWith
func tlogBlockComparator(a, b interface{}) int {
	tlbA := a.(*schema.TlogBlock)
	tlbB := b.(*schema.TlogBlock)

	seqA, seqB := tlbA.Sequence(), tlbB.Sequence()

	switch {
	case seqA < seqB:
		return -1
	case seqA > seqB:
		return 1
	default: // tlbA.Sequence() == tlbB.Sequence():
		return 0
	}
}

// this is the flusher routine that receive the blocks and order it.
func (vd *vdisk) runReceiver() {
	buffer := treeset.NewWith(tlogBlockComparator)

	for {
		tlb := <-vd.blockInputChan

		curSeq := tlb.Sequence()

		if curSeq == vd.expectedSequence {
			// it is the next sequence we wait
			vd.orderedBlockChan <- tlb
			vd.expectedSequence++
		} else if curSeq > vd.expectedSequence {
			// if this is not what we wait, buffer it
			buffer.Add(tlb)
		} else { // tlb.Sequence() < vd.expectedSequence -> duplicated message
			continue // drop it and no need to check the buffer because all condition doesn't change
		}

		if buffer.Empty() {
			continue
		}

		// lets see the buffer again, check if we have blocks that
		// can be sent to the flusher.
		it := buffer.Iterator()
		blocks := []*schema.TlogBlock{}

		expected := vd.expectedSequence
		for it.Next() {
			block := it.Value().(*schema.TlogBlock)
			if block.Sequence() != expected {
				break
			}
			expected++
			blocks = append(blocks, block)
		}

		for _, block := range blocks {
			buffer.Remove(block) // we can only do it here because the iterator is read only
			vd.orderedBlockChan <- block
			vd.expectedSequence++
		}
	}
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) runFlusher() {
	var err error

	tlogs := []*schema.TlogBlock{}
	dur := time.Duration(vd.flusher.flushTime) * time.Second
	pfTimer := time.NewTimer(dur) // periodic flush timer

	var toFlushLen int
	for {
		select {
		case tlb := <-vd.orderedBlockChan:
			tlogs = append(tlogs, tlb)
			if len(tlogs) < vd.flusher.flushSize { // only flush if it > f.flushSize
				continue
			}
			toFlushLen = vd.flusher.flushSize

			pfTimer.Stop()
			pfTimer.Reset(dur)

		case <-pfTimer.C:
			pfTimer.Reset(dur)
			if len(tlogs) == 0 {
				continue
			}
			toFlushLen = len(tlogs)

		case cmd := <-vd.cmdChan:
			if cmd != tlog.MessageForceFlush {
				log.Errorf("invalid command to runFlusher: %v", cmd)
				continue
			}

			if len(tlogs) == 0 {
				continue
			}
			pfTimer.Stop()
			pfTimer.Reset(dur)

			toFlushLen = len(tlogs)
		}

		// get the blocks
		blocks := tlogs[:toFlushLen]
		tlogs = tlogs[toFlushLen:]

		var seqs []uint64
		status := tlog.BlockStatusFlushOK

		seqs, err = vd.flusher.flush(blocks[:], vd)
		if err != nil {
			log.Infof("flush %v failed: %v", vd.vdiskID, err)
			status = tlog.BlockStatusFlushFailed
		} else {
			vd.lastSeqFlushed = seqs[len(seqs)-1] // update our last sequence flushed
		}

		vd.respChan <- &BlockResponse{
			Status:    status.Int8(),
			Sequences: seqs,
		}
	}
}
