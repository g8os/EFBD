package server

import (
	"sync"
	"time"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
)

const (
	respChanSize = 10

	// tlogblock buffer size = flusher.flushSize * tlogBlockFactorSize
	// With buffer size that bigger than flushSize:
	// - we don't always block when flushing
	// - our RAM won't exploded because we still have upper limit
	tlogBlockFactorSize = 5
)

var vdiskMgr *vdiskManager

type vdisk struct {
	vdiskID    string
	lastHash   []byte
	inputChan  chan *schema.TlogBlock
	respChan   chan *response
	flusher    *flusher
	segmentBuf []byte
}

func newVdisk(vdiskID string, f *flusher) (*vdisk, error) {
	// get last hash from storage
	lastHash, err := f.getLastHash(vdiskID)
	if err != nil {
		return nil, err
	}

	return &vdisk{
		vdiskID:   vdiskID,
		lastHash:  lastHash,
		inputChan: make(chan *schema.TlogBlock, f.flushSize*tlogBlockFactorSize),
		respChan:  make(chan *response, respChanSize),
		flusher:   f,
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

func (vt *vdiskManager) get(vdiskID string, f *flusher) (*vdisk, error) {
	vt.lock.Lock()
	defer vt.lock.Unlock()

	vd, ok := vt.vdisks[vdiskID]
	if !ok {
		var err error
		vd, err = newVdisk(vdiskID, f)
		if err != nil {
			return nil, err
		}
		go vd.Run()
	}

	return vd, nil
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) Run() {
	var err error

	tlogs := []*schema.TlogBlock{}
	dur := time.Duration(vd.flusher.flushTime) * time.Second
	pfTimer := time.NewTimer(dur) // periodic flush timer

	var toFlushLen int
	for {
		select {
		case tlb := <-vd.inputChan:
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
		}

		// get the blocks
		blocks := tlogs[:toFlushLen]
		tlogs = tlogs[toFlushLen:]

		var seqs []uint64
		var status int8 = tlog.StatusFlushOK

		seqs, err = vd.flusher.flush(blocks[:], vd)
		if err != nil {
			log.Infof("flush %v failed: %v", vd.vdiskID, err)
			status = tlog.StatusFlushFailed
		}

		vd.respChan <- &response{
			Status:    status,
			Sequences: seqs,
		}
	}
}

// resize segmentBuf to new length
func (vd *vdisk) resizeSegmentBuf(length int) {
	if length > vdiskMgr.maxSegmentBufLen {
		length = vdiskMgr.maxSegmentBufLen
	}

	if length <= cap(vd.segmentBuf) {
		return
	}

	vd.segmentBuf = make([]byte, 0, length)
}
