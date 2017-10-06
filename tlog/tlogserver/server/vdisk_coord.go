package server

import (
	"net"

	"github.com/zero-os/0-Disk/log"
)

func (vd *vdisk) coordOther() (uint64, error) {
	vd.waitOther()

	if err := vd.createFlusher(); err != nil {
		return 0, err
	}
	return vd.loadLastFlushedSequence()
}

// waitOther wait for other server to finished
func (vd *vdisk) waitOther() {
	defer func() {
		log.Infof("waitOther finished")
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
