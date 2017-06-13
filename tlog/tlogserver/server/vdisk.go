package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/treeset"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
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
	vdiskCmdForceFlushBlocking           = iota // blocking force flush
	vdiskCmdForceFlushAtSeq                     // non-blocking force flush with sequence param
	vdiskCmdClearUnorderedBlocksBlocking        // blocking clear all unordered blocks
)

// command for vdisk flusher
type vdiskFlusherCmd struct {
	cmdType  int8
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

	flusher              *flusher
	segmentBuf           []byte // capnp segment buffer used by the flusher
	expectedSequence     uint64 // expected sequence to be received
	expectedSequenceLock sync.Mutex

	clientsTab     map[string]*net.TCPConn
	clientsTabLock sync.Mutex

	aggToProcessCh  chan aggmq.AggMqMsg
	withSlaveSyncer bool
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
func newVdisk(vdiskConf config.VdiskConfig, aggMq *aggmq.MQ, vdiskID string, f *flusher, firstSequence uint64,
	flusherConf *flusherConfig, segmentBufLen int) (*vdisk, error) {

	var aggToProcessCh chan aggmq.AggMqMsg
	var withSlaveSyncer bool

	// create aggregation processor
	if aggMq != nil && vdiskConf.TlogSlaveSync {
		var err error
		apc := aggmq.AggProcessorConfig{
			VdiskID:  vdiskID,
			K:        flusherConf.K,
			M:        flusherConf.M,
			PrivKey:  flusherConf.PrivKey,
			HexNonce: flusherConf.HexNonce,
		}
		ctx, _ := context.WithCancel(context.Background()) // TODO : save and use the context properly

		aggToProcessCh, err = aggMq.AskProcessor(ctx, apc)
		if err != nil {
			return nil, err
		}
		withSlaveSyncer = true
	}

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
		withSlaveSyncer:      withSlaveSyncer,
		aggToProcessCh:       aggToProcessCh,
	}, nil
}

type vdiskManager struct {
	vdisks           map[string]*vdisk
	lock             sync.Mutex
	aggMq            *aggmq.MQ
	configPath       string
	maxSegmentBufLen int // max len of capnp buffer used by flushing process
}

func newVdiskManager(aggMq *aggmq.MQ, blockSize, flushSize int, configPath string) *vdiskManager {
	// the estimation of max segment buf len we will need.
	// we add it by '1' because:
	// - the block will also container other data like 'sequenece', 'timestamp', etc..
	// - overhead of capnp schema
	segmentBufLen := blockSize * (flushSize + 1)

	return &vdiskManager{
		aggMq:            aggMq,
		vdisks:           map[string]*vdisk{},
		maxSegmentBufLen: segmentBufLen,
		configPath:       configPath,
	}
}

type flusherFactory func(vdiskID string, flusherConf *flusherConfig) (*flusher, error)

// get or create the vdisk
func (vt *vdiskManager) Get(globalConf *config.Config, vdiskID string, firstSequence uint64, ff flusherFactory,
	conn *net.TCPConn, flusherConf *flusherConfig) (vd *vdisk, err error) {

	vt.lock.Lock()
	defer vt.lock.Unlock()

	// get vdisk config
	vdiskConf, ok := globalConf.Vdisks[vdiskID]
	if !ok {
		return nil, fmt.Errorf("config for vdisk `%v` not found", vdiskID)
	}

	// check if this vdisk already exist
	vd, ok = vt.vdisks[vdiskID]
	if ok {
		vd.addClient(conn)
		return
	}

	// create the flusher
	f, err := ff(vdiskID, flusherConf)
	if err != nil {
		return
	}

	// create vdisk
	vd, err = newVdisk(vdiskConf, vt.aggMq, vdiskID, f, firstSequence, flusherConf, vt.maxSegmentBufLen)
	if err != nil {
		return
	}
	vd.addClient(conn)
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	// run vdisk goroutines
	go vd.runFlusher()
	go vd.runBlockReceiver()

	return
}

// number of connected clients to this vdisk
func (vd *vdisk) numConnectedClient() int {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()
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
	vd.expectedSequenceLock.Lock()
	defer vd.expectedSequenceLock.Unlock()

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
			func() {
				vd.expectedSequenceLock.Lock()
				defer vd.expectedSequenceLock.Unlock()

				curSeq := tlb.Sequence()

				if curSeq == vd.expectedSequence {
					// it is the next sequence we wait
					vd.orderedBlockChan <- tlb
					vd.expectedSequence++
				} else if curSeq > vd.expectedSequence {
					// if this is not what we wait, buffer it
					buffer.Add(tlb)
				} else { // tlb.Sequence() < vd.expectedSequence -> duplicated message
					return // drop it and no need to check the buffer because all condition doesn't change
				}

				if buffer.Empty() {
					return
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
			}()
		}
	}
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) runFlusher() {
	var err error

	// buffer of all ordered tlog blocks
	tlogs := []*schema.TlogBlock{}
	var maxSeq uint64

	// periodic flush interval
	pfDur := time.Duration(vd.flusher.flushTime) * time.Second

	// periodic flush timer
	pfTimer := time.NewTimer(pfDur)

	var toFlushLen int
	var flusherCmd vdiskFlusherCmd
	var cmdType int8

	var seqToForceFlush uint64 // sequence to be force flushed
	var needForceFlushSeq bool // true if we wait for a sequence to be force flushed

	for {
		cmdType = -1
		select {
		case tlb := <-vd.orderedBlockChan:
			tlogs = append(tlogs, tlb)

			maxSeq = tlb.Sequence()
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
				if maxSeq < seqToForceFlush { // we don't have it yet
					needForceFlushSeq = true
					continue
				}
				// we already have the wanted sequence
				// flush right now if possible
				needForceFlushSeq = false
			case vdiskCmdForceFlushBlocking:
			default:
				log.Errorf("invalid command to runFlusher: %v", flusherCmd)
				continue
			}

			if len(tlogs) == 0 {
				if cmdType == vdiskCmdForceFlushBlocking {
					// if it is blocking cmd, something else is waiting, notify him!
					vd.flusherCmdRespChan <- struct{}{}
				}
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
		var rawAgg []byte

		seqs, rawAgg, err = vd.flusher.flush(blocks[:], vd)
		if err != nil {
			log.Infof("flush %v failed: %v", vd.vdiskID, err)
			status = tlog.BlockStatusFlushFailed
		}

		if cmdType == vdiskCmdForceFlushBlocking {
			// if it is blocking cmd, something else is waiting, notify him!
			vd.flusherCmdRespChan <- struct{}{}
		}

		// send response
		vd.respChan <- &BlockResponse{
			Status:    status.Int8(),
			Sequences: seqs,
		}

		// send aggregation to slave syncer
		if vd.withSlaveSyncer {
			vd.aggToProcessCh <- aggmq.AggMqMsg(rawAgg)
		}
	}
}
