package server

import (
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
func (vt *vdiskManager) Get(fileConfig *config.Config, vdiskID string, firstSequence uint64, ff flusherFactory,
	conn *net.TCPConn, flusherConf *flusherConfig) (vd *vdisk, err error) {

	vt.lock.Lock()
	defer vt.lock.Unlock()

	// check if this vdisk already exist
	vd, ok := vt.vdisks[vdiskID]
	if ok {
		vd.addClient(conn)
		return
	}

	// create the flusher
	f, err := ff(vdiskID, flusherConf)
	if err != nil {
		return
	}

	withSlaveSync := func() bool {
		if fileConfig != nil {
			if vdiskConf, ok := fileConfig.Vdisks[vdiskID]; ok {
				return vdiskConf.TlogSlaveSync
			}
		}
		return false
	}()

	// create vdisk
	vd, err = newVdisk(vt.aggMq, vdiskID, f, firstSequence, flusherConf, vt.maxSegmentBufLen, withSlaveSync)
	if err != nil {
		return
	}
	vd.addClient(conn)
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	return
}
