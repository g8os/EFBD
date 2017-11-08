package server

import (
	"context"
	"net"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
)

type vdiskManager struct {
	vdisks       map[string]*vdisk
	lock         sync.Mutex
	configSource config.Source
	slaveSyncMgr tlog.SlaveSyncerManager
}

func newVdiskManager(slaveSyncMgr tlog.SlaveSyncerManager, flushSize int, configSource config.Source) *vdiskManager {
	return &vdiskManager{
		slaveSyncMgr: slaveSyncMgr,
		vdisks:       map[string]*vdisk{},
		configSource: configSource,
	}
}

// get or create the vdisk
func (vt *vdiskManager) Get(ctx context.Context, vdiskID string,
	conn *net.TCPConn, flusherConf *flusherConfig, coordConnectAddr string) (vd *vdisk, err error) {

	vt.lock.Lock()
	defer vt.lock.Unlock()

	// check if this vdisk already exist
	vd, ok := vt.vdisks[vdiskID]
	if ok {
		return
	}

	// create vdisk
	vd, err = newVdisk(ctx, vdiskID, vt.slaveSyncMgr, vt.configSource,
		flusherConf, vt.remove, coordConnectAddr)
	if err != nil {
		return
	}
	vt.vdisks[vdiskID] = vd

	log.Debugf("create vdisk with expectedSequence:%v", vd.expectedSequence)

	return
}

// exists returns true if vdisk with given ID is exists
func (vt *vdiskManager) exists(vdiskID string) bool {
	vt.lock.Lock()
	defer vt.lock.Unlock()
	_, exists := vt.vdisks[vdiskID]
	return exists
}

func (vt *vdiskManager) remove(vdiskID string) {
	vt.lock.Lock()
	defer vt.lock.Unlock()
	delete(vt.vdisks, vdiskID)
}
