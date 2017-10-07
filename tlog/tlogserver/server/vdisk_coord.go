package server

import (
	"net"

	"github.com/zero-os/0-Disk/log"
)

// waitOther to finish
// it returns the new last flushed sequence
func (vd *vdisk) waitOther() (uint64, error) {
	vd.waitOtherToFinish()

	if err := vd.createFlusher(); err != nil {
		return 0, err
	}
	return vd.loadLastFlushedSequence()
}

// wait for other server to be finished
func (vd *vdisk) waitOtherToFinish() {
	defer func() {
		log.Infof("waitOther %v finished", vd.coordConnectAddr)
	}()

	conn, err := net.Dial("tcp", vd.coordConnectAddr)
	if err != nil {
		log.Errorf("failed to dial %v: %v", vd.coordConnectAddr, err)
		return
	}
	log.Infof("[coord] waiting for %v to dies", vd.coordConnectAddr)
	b := make([]byte, 10)
	_, err = conn.Read(b)
	if err != nil {
		log.Errorf("failed to read:%v", err)
	}

}
