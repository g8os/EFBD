package server

import (
	"net"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// waitOther to finish
// it returns the new last flushed sequence
func (vd *vdisk) waitOther() (uint64, error) {
	if err := vd.waitOtherToFinish(); err != nil {
		return 0, err
	}

	if err := vd.createFlusher(); err != nil {
		return 0, err
	}
	return vd.loadLastFlushedSequence()
}

// wait for other server to be finished
func (vd *vdisk) waitOtherToFinish() error {
	defer log.Infof("waitOther %v finished", vd.coordConnectAddr)

	// connect to the other server
	conn, err := net.Dial("tcp", vd.coordConnectAddr)
	if err != nil {
		log.Errorf("failed to dial %v: %v", vd.coordConnectAddr, err)
		return nil
	}

	log.Infof("vdisk %v waiting for remote server %v to dies", vd.id, vd.coordConnectAddr)

	needWait, err := vd.waitOtherHandshake(conn)
	if !needWait || err != nil {
		return err
	}

	// wait for other server to dies
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		log.Errorf("failed to read:%v", err)
	}

	return nil
}

func (vd *vdisk) waitOtherHandshake(conn net.Conn) (bool, error) {
	// send handshake request
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return false, err
	}
	handshake, err := schema.NewRootWaitTlogHandshakeRequest(seg)
	if err != nil {
		return false, err
	}
	if err := handshake.SetVdiskID(vd.id); err != nil {
		return false, err
	}
	if err := capnp.NewEncoder(conn).Encode(msg); err != nil {
		return false, err
	}

	// read handshake response
	msg, err = capnp.NewDecoder(conn).Decode()
	if err != nil {
		return false, err
	}
	resp, err := schema.ReadRootWaitTlogHandshakeResponse(msg)
	if err != nil {
		return false, err
	}

	return resp.Exists(), nil
}
