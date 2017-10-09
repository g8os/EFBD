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

	// connect to the other server
	conn, err := net.Dial("tcp", vd.coordConnectAddr)
	if err != nil {
		log.Errorf("failed to dial %v: %v", vd.coordConnectAddr, err)
		return
	}

	log.Infof("vdisk %v waiting for remote server %v to dies", vd.id, vd.coordConnectAddr)

	// wait for other server to dies
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		log.Errorf("failed to read:%v", err)
	}

}
