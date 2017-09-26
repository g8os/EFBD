package server

import (
	"context"
	"net"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

type vdiskManager struct {
	vdisks           map[string]*vdisk
	lock             sync.Mutex
	aggMq            *aggmq.MQ
	configSource     config.Source
	maxSegmentBufLen int // max len of capnp buffer used by flushing process
}

func newVdiskManager(aggMq *aggmq.MQ, blockSize, flushSize int, configSource config.Source) *vdiskManager {
	// the estimation of max segment buf len we will need.
	// we add it by '1' because:
	// - the block will also container other data like 'sequenece', 'timestamp', etc..
	// - overhead of capnp schema
	segmentBufLen := blockSize * (flushSize + 1)

	return &vdiskManager{
		aggMq:            aggMq,
		vdisks:           map[string]*vdisk{},
		maxSegmentBufLen: segmentBufLen,
		configSource:     configSource,
	}
}

// get or create the vdisk
func (vt *vdiskManager) Get(ctx context.Context, vdiskID string,
	conn *net.TCPConn, flusherConf *flusherConfig) (vd *vdisk, err error) {

	vt.lock.Lock()
	defer vt.lock.Unlock()

	// check if this vdisk already exist
	vd, ok := vt.vdisks[vdiskID]
	if ok {
		return
	}

	// create vdisk
	vd, err = newVdisk(ctx, vdiskID, vt.aggMq, vt.configSource,
		flusherConf, vt.remove)
	if err != nil {
		return
	}
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	return
}

func (vt *vdiskManager) remove(vdiskID string) {
	vt.lock.Lock()
	defer vt.lock.Unlock()
	delete(vt.vdisks, vdiskID)
}
