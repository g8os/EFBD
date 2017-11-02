package server

import (
	"bufio"
	"context"
	"io"
	"net"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (vd *vdisk) handle(conn *net.TCPConn, br *bufio.Reader, respSegmentBufLen int) error {
	ctx, cancelFunc := context.WithCancel(vd.ctx)
	defer func() {
		vd.removeConn(conn)
		cancelFunc()
		log.Infof("vdisk `%v` connection handler exited", vd.id)
	}()

	// start response sender
	go vd.sendResp(ctx, conn, respSegmentBufLen)

	if !vd.Ready() && vd.coordConnectAddr != "" {
		lastSeq, err := vd.waitOther()
		if err != nil {
			return err
		}

		vd.mux.Lock()
		vd.ready = true
		vd.mux.Unlock()

		// tell client that we are ready
		vd.respChan <- &BlockResponse{
			Status:    tlog.BlockStatusReady.Int8(),
			Sequences: []uint64{lastSeq},
		}
	}

	for {
		// decode message
		msg, err := capnp.NewDecoder(br).Decode()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil // EOF in this stage is not an error
			}
			return err
		}

		cmd, err := schema.ReadRootTlogClientMessage(msg)
		if err != nil {
			return err
		}

		// handle message according to it's typ

		switch which := cmd.Which(); which {
		case schema.TlogClientMessage_Which_block:
			block, bErr := cmd.Block()
			if bErr != nil {
				err = bErr
			} else {
				err = vd.handleBlock(&block)
			}

		case schema.TlogClientMessage_Which_forceFlushAtSeq:
			err = vd.handleForceFlushAtSeq(cmd.ForceFlushAtSeq())

		case schema.TlogClientMessage_Which_waitNBDSlaveSync:
			err = vd.handleWaitNBDSlaveSync()
		case schema.TlogClientMessage_Which_disconnect:
			log.Infof("vdisk `%v` receive disconnect command", vd.id)
			return nil
		default:
			err = errors.Newf("%v is not a supported client message type", which)
		}

		if err != nil {
			log.Errorf("vd handle failed: %v", err)
			return err
		}
	}
}

// response sender for a vdisk
func (vd *vdisk) sendResp(ctx context.Context, conn *net.TCPConn, respSegmentBufLen int) {
	segmentBuf := make([]byte, 0, respSegmentBufLen)

	defer func() {
		log.Infof("sendResp cleanup for vdisk %v", vd.id)
		resp := BlockResponse{
			Status: tlog.BlockStatusDisconnected.Int8(),
		}
		if err := resp.Write(conn, segmentBuf); err != nil {
			log.Errorf("failed to send disconnect command: %v", err)
		}

		conn.Close() // it will also close the receiver
	}()

	for {
		select {
		case resp := <-vd.respChan:
			if err := resp.Write(conn, segmentBuf); err != nil && resp != nil {
				log.Infof("failed to send resp to :%v, err:%v", vd.id, err)
				return
			}

		case <-ctx.Done():
			log.Debugf("abort current sendResp goroutine for vdisk:%v", vd.id)
			return
		}
	}
}

func (vd *vdisk) handleBlock(block *schema.TlogBlock) error {
	// check hash
	if err := vd.hash(block); err != nil {
		log.Debugf("hash check failed:%v\n", err)
		return err
	}

	seq := block.Sequence()

	vd.mux.Lock()
	defer vd.mux.Unlock()

	if seq < vd.expectedSequence {
		vd.respChan <- &BlockResponse{
			Status:    tlog.BlockStatusRecvOK.Int8(),
			Sequences: []uint64{block.Sequence()},
		}
		return nil
	}
	if seq > vd.expectedSequence {
		return nil
	}

	vd.expectedSequence++

	// store
	vd.orderedBlockChan <- block
	vd.respChan <- &BlockResponse{
		Status:    tlog.BlockStatusRecvOK.Int8(),
		Sequences: []uint64{block.Sequence()},
	}
	return nil
}

func (vd *vdisk) handleForceFlushAtSeq(sequence uint64) error {
	vd.forceFlushAtSeq(sequence)
	vd.respChan <- &BlockResponse{
		Status: tlog.BlockStatusForceFlushReceived.Int8(),
	}
	return nil
}

func (vd *vdisk) handleWaitNBDSlaveSync() error {
	vd.waitSlaveSync()
	log.Debugf("sending BlockStatusWaitNbdSlaveSyncReceived to vdisk: %v", vd.id)
	vd.respChan <- &BlockResponse{
		Status: tlog.BlockStatusWaitNbdSlaveSyncReceived.Int8(),
	}
	return nil
}

// hash tlog data and check against given hash from client
func (vd *vdisk) hash(tlb *schema.TlogBlock) (err error) {
	data, err := tlb.Data()
	if err != nil {
		return
	}

	// get expected hash
	rawHash, err := tlb.Hash()
	if err != nil {
		return
	}
	expectedHash := zerodisk.Hash(rawHash)

	// compute hashs based on given data
	hash := zerodisk.HashBytes(data)

	if !expectedHash.Equals(hash) {
		err = errors.New("data hash is incorrect")
		return
	}

	return
}

func (vd *vdisk) attachConn(conn *net.TCPConn) error {
	vd.clientConnLock.Lock()
	defer vd.clientConnLock.Unlock()

	if vd.clientConn != nil {
		return errors.Newf("there is already conencted client for vdisk `%v`", vd.id)
	}

	vd.clientConn = conn
	return nil
}

func (vd *vdisk) removeConn(conn *net.TCPConn) error {
	vd.clientConnLock.Lock()
	defer vd.clientConnLock.Unlock()

	vd.clientConn = nil
	return nil

}
