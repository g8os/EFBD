package server

import (
	"net"

	"github.com/zero-os/0-Disk/log"
)

func (vd *vdisk) coordOther(addr string) (uint64, error) {
	vd.waitOther(addr)

	if err := vd.createFlusher(); err != nil {
		return 0, err
	}
	return vd.loadLastFlushedSequence()
}

// waitOther wait for other server to finished
func (vd *vdisk) waitOther(addr string) {
	defer func() {
		log.Infof("waitOther finished")
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Errorf("failed to dial %v: %v", addr, err)
		return
	}
	log.Infof("[coord] waiting for %v to dies", addr)
	b := make([]byte, 10)
	_, err = conn.Read(b)
	if err != nil {
		log.Errorf("failed to read:%v", err)
	}

}
