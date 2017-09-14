package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (vd *vdisk) handle(conn *net.TCPConn, br *bufio.Reader, respSegmentBufLen int) error {
	ctx, cancelFunc := context.WithCancel(vd.ctx)
	defer cancelFunc()

	// start response sender
	go vd.sendResp(ctx, conn, respSegmentBufLen)

	for {
		msg, err := capnp.NewDecoder(br).Decode()
		if err != nil {
			if err == io.EOF {
				return nil // EOF in this stage is not an error
			}
			return err
		}

		cmd, err := schema.ReadRootTlogClientMessage(msg)
		if err != nil {
			return err
		}

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

		default:
			err = fmt.Errorf("%v is not a supported client message type", which)
		}

		if err != nil {
			return err
		}
	}
}

// response sender for a vdisk
func (vd *vdisk) sendResp(ctx context.Context, conn *net.TCPConn, respSegmentBufLen int) {
	defer func() {
		log.Infof("sendResp cleanup for vdisk %v", vd.id)
		conn.Close() // it will also close the receiver
	}()

	segmentBuf := make([]byte, 0, respSegmentBufLen)

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

	// store
	vd.blockInputChan <- block
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

// number of connected clients to this vdisk
func (vd *vdisk) numConnectedClient() int {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()
	return len(vd.clientsTab)
}

// add client to the table of connected clients
func (vd *vdisk) addClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	vd.clientsTab[conn.RemoteAddr().String()] = conn
}

// remove client from the table of connected clients
func (vd *vdisk) removeClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	addr := conn.RemoteAddr().String()
	if _, ok := vd.clientsTab[addr]; !ok {
		log.Errorf("vdisk failed to remove client:%v", addr)
	} else {
		delete(vd.clientsTab, addr)
	}
}

// disconnect all connected clients except the
// given exceptConn connection
func (vd *vdisk) disconnectExcept(exceptConn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	var conns []*net.TCPConn
	for _, conn := range vd.clientsTab {
		if conn != exceptConn {
			conns = append(conns, conn)
			conn.Close()
		}
	}
	for _, conn := range conns {
		delete(vd.clientsTab, conn.RemoteAddr().String())
	}
}
