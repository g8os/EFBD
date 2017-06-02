package server

import (
	"net"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/treeset"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
)

const (
	respChanSize = 10

	// tlogblock buffer size = flusher.flushSize * tlogBlockFactorSize
	// With buffer size that bigger than flushSize:
	// - we don't always block when flushing
	// - our RAM won't exploded because we still have upper limit
	tlogBlockFactorSize = 5
)

const (
	vdiskCmdForceFlush                   = iota // non-blocking force flush
	vdiskCmdForceFlushBlocking                  // blocking force flush
	vdiskCmdForceFlushAtSeq                     // non-blocking force flush with sequence param
	vdiskCmdClearUnorderedBlocksBlocking        // blocking clear all unordered blocks
)

// command for vdisk flusher
type vdiskFlusherCmd struct {
	cmdType  uint8
	sequence uint64
}

type vdisk struct {
	vdiskID     string
	lastHash    []byte // current in memory last hash
	lastHashKey []byte // redis key of the last hash

	// channels block receiver
	blockInputChan       chan *schema.TlogBlock // input channel of block received from client
	blockRecvCmdChan     chan uint8             // channel of block receiver command
	blockRecvCmdRespChan chan struct{}          // channel of block receiver command response

	// channels for flusher
	orderedBlockChan   chan *schema.TlogBlock // ordered blocks from blockInputChan
	flusherCmdChan     chan vdiskFlusherCmd   // channel of flusher command
	flusherCmdRespChan chan struct{}          // channel of flusher command response
	respChan           chan *BlockResponse    // channel of responses to be sent to client

	flusher          *flusher
	segmentBuf       []byte // capnp segment buffer used by the flusher
	expectedSequence uint64 // expected sequence to be received
	lastSeqFlushed   uint64 // last sequence flushed
	clientsTab       map[string]*net.TCPConn
	clientsTabLock   sync.Mutex
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
		vdiskID:              vdiskID,
		lastHashKey:          decoder.GetLashHashKey(vdiskID),
		lastHash:             lastHash,
		blockInputChan:       make(chan *schema.TlogBlock, maxTlbInBuffer),
		blockRecvCmdChan:     make(chan uint8, 1),
		blockRecvCmdRespChan: make(chan struct{}, 1),
		orderedBlockChan:     make(chan *schema.TlogBlock, maxTlbInBuffer),
		flusherCmdChan:       make(chan vdiskFlusherCmd, 1),
		flusherCmdRespChan:   make(chan struct{}, 1),
		respChan:             make(chan *BlockResponse, respChanSize),
		expectedSequence:     firstSequence,
		segmentBuf:           make([]byte, 0, segmentBufLen),
		flusher:              f,
		clientsTab:           make(map[string]*net.TCPConn),
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
func (vt *vdiskManager) Get(vdiskID string, firstSequence uint64, ff flusherFactory,
	conn *net.TCPConn) (vd *vdisk, err error) {
	vt.lock.Lock()
	defer vt.lock.Unlock()

	vd, ok := vt.vdisks[vdiskID]
	if ok {
		vd.addClient(conn)
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
	vd.addClient(conn)
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	go vd.runFlusher()
	go vd.runBlockReceiver()

	return
}

// number of connected clients to this vdisk
func (vd *vdisk) numConnectedClient() int {
	return len(vd.clientsTab)
}

func (vd *vdisk) addClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	vd.clientsTab[conn.RemoteAddr().String()] = conn
}

func (vd *vdisk) removeClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	addr := conn.RemoteAddr().String()
	if _, ok := vd.clientsTab[addr]; !ok {
		log.Errorf("vdisk failed to remove client:%v", addr)
	} else {
		delete(vd.clientsTab, addr)
	}
}

// disconnect all connected clients except the
// given exceptConn connection
func (vd *vdisk) disconnectExcept(exceptConn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	var conns []*net.TCPConn
	for _, conn := range vd.clientsTab {
		if conn != exceptConn {
			conns = append(conns, conn)
			conn.Close()
		}
	}
	for _, conn := range conns {
		delete(vd.clientsTab, conn.RemoteAddr().String())
	}
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

// reset first sequence, what we need to do:
// - make sure we only have single connections for this vdisk
// - ignore all unordered blocks (blocking)
// - flush all unflushed ordered blocks (blocking)
// - reset it
func (vd *vdisk) resetFirstSequence(newSeq uint64, conn *net.TCPConn) error {
	log.Infof("vdisk %v reset first sequence to %v", vd.vdiskID, newSeq)
	if newSeq == vd.expectedSequence { // we already in same page, do nothing
		return nil
	}

	if vd.numConnectedClient() != 1 {
		vd.disconnectExcept(conn)
	}

	// send ignore all unordered blocks command
	vd.blockRecvCmdChan <- vdiskCmdClearUnorderedBlocksBlocking
	<-vd.blockRecvCmdRespChan

	// send flush command
	vd.flusherCmdChan <- vdiskFlusherCmd{
		cmdType: vdiskCmdForceFlushBlocking,
	}
	<-vd.flusherCmdRespChan

	vd.expectedSequence = newSeq
	return nil
}

// force flush right now
func (vd *vdisk) forceFlush() {
	vd.flusherCmdChan <- vdiskFlusherCmd{
		cmdType: vdiskCmdForceFlush,
	}
}

// force flush when vdisk receive the given sequence
func (vd *vdisk) forceFlushAtSeq(seq uint64) {
	vd.flusherCmdChan <- vdiskFlusherCmd{
		cmdType:  vdiskCmdForceFlushAtSeq,
		sequence: seq,
	}
}

// this is the flusher routine that receive the blocks and order it.
func (vd *vdisk) runBlockReceiver() {
	buffer := treeset.NewWith(tlogBlockComparator)

	for {
		select {
		case cmd := <-vd.blockRecvCmdChan:
			if cmd == vdiskCmdClearUnorderedBlocksBlocking {
				buffer.Clear()
				vd.blockRecvCmdRespChan <- struct{}{}
			} else {
				log.Errorf("invalid block receiver command = %v", cmd)
			}
		case tlb := <-vd.blockInputChan:
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
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) runFlusher() {
	var err error

	// buffer of all ordered tlog blocks
	tlogs := []*schema.TlogBlock{}

	// periodic flush interval
	pfDur := time.Duration(vd.flusher.flushTime) * time.Second

	// periodic flush timer
	pfTimer := time.NewTimer(pfDur)

	var toFlushLen int
	var flusherCmd vdiskFlusherCmd
	var cmdType uint8

	var seqToForceFlush uint64 // sequence to be force flushed
	var needForceFlushSeq bool // true if we wait for a sequence to be force flushed

	for {
		select {
		case tlb := <-vd.orderedBlockChan:
			tlogs = append(tlogs, tlb)

			// check if we need to flush
			if needForceFlushSeq && tlb.Sequence() >= seqToForceFlush {
				// reset the flag and flush right now
				needForceFlushSeq = false
				toFlushLen = len(tlogs)
			} else if len(tlogs) < vd.flusher.flushSize {
				// only flush if it > f.flushSize
				continue
			} else {
				toFlushLen = vd.flusher.flushSize
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)

		case <-pfTimer.C:
			pfTimer.Reset(pfDur)
			if len(tlogs) == 0 {
				continue
			}
			toFlushLen = len(tlogs)

		case flusherCmd = <-vd.flusherCmdChan:
			cmdType = flusherCmd.cmdType

			switch cmdType {
			case vdiskCmdForceFlushAtSeq:
				seqToForceFlush = flusherCmd.sequence
				if vd.expectedSequence <= seqToForceFlush { // we don't have it yet
					needForceFlushSeq = true
					continue
				} else {
					needForceFlushSeq = false
					// we already have it, do force flush right now if possible
					if len(tlogs) == 0 { // oh, tlogs buffer is empty, it means it already flushed
					}
				}

			case vdiskCmdForceFlush, vdiskCmdForceFlushBlocking:
				if len(tlogs) == 0 {
					if cmdType == vdiskCmdForceFlushBlocking {
						vd.flusherCmdRespChan <- struct{}{}
					}
					continue
				}

			default:
				log.Errorf("invalid command to runFlusher: %v", flusherCmd)
				continue
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)

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

		if cmdType == vdiskCmdForceFlushBlocking {
			// if it is blocking cmd, something else is waiting, notify him!
			vd.flusherCmdRespChan <- struct{}{}
		}

		vd.respChan <- &BlockResponse{
			Status:    status.Int8(),
			Sequences: seqs,
		}
	}
}
