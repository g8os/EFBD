package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

// Server defines a tlog server
type Server struct {
	port                 int
	bufSize              int
	maxRespSegmentBufLen int // max len of response capnp segment buffer
	f                    *flusher
	ObjStorAddresses     []string
	listener             net.Listener
}

// NewServer creates a new tlog server
func NewServer(conf *Config) (*Server, error) {
	f, err := newFlusher(conf)
	if err != nil {
		return nil, err
	}

	objstorAddrs, err := conf.ObjStoreServerAddresses()
	if err != nil {
		return nil, err
	}

	var listener net.Listener
	if conf.ListenAddr != "" {
		// listen for tcp requests on given address
		listener, err = net.Listen("tcp", conf.ListenAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen to %v: %v", conf.ListenAddr, err)
		}
	} else {
		// listen for tcp requests on localhost using any available port
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			if listener, err = net.Listen("tcp6", "[::1]:0"); err != nil {
				return nil, fmt.Errorf("failed to listen on localhost, port: %v", err)
			}
		}
	}

	vdiskMgr = newVdiskManager(conf.BlockSize, conf.FlushSize)
	return &Server{
		f:                    f,
		ObjStorAddresses:     objstorAddrs,
		listener:             listener,
		maxRespSegmentBufLen: schema.RawTlogRespLen(conf.FlushSize),
	}, nil
}

// Listen to incoming (tcp) Requests
func (s *Server) Listen() {
	defer s.listener.Close()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Infof("couldn't accept connection: %v", err)
			continue
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			log.Info("received conn is not tcp conn")
			continue
		}
		go s.handle(tcpConn)
	}
}

// ListenAddr returns the address the (tcp) server is listening on
func (s *Server) ListenAddr() string {
	return s.listener.Addr().String()
}

// handshake stage, required prior to receiving blocks
func (s *Server) handshake(r io.Reader, w io.Writer) (cfg *ConnectionConfig, err error) {
	verack, err := s.readDecodeVerAck(r)
	if err != nil {
		return
	}

	log.Debug("received verack of incoming connection")

	segmentBuf := make([]byte, 0, schema.RawServerVerackLen())

	// validate version
	clientVersion := blockstor.Version(verack.Version())
	if clientVersion.Compare(tlog.MinSupportedVersion) < 0 {
		err = s.writeVerackResponse(w, segmentBuf, tlog.VerackStatusInvalidVersion)
		if err != nil {
			log.Infof("couldn't write server invalid-version response: %s", err.Error())
		}
		err = errors.New("client version is not supported by this server")
		return
	}
	log.Debug("incoming connection checks out with version:", clientVersion.String())

	// make sure vdisk doesn't exist yet
	vdiskID, err := verack.VdiskID()
	if err != nil {
		writeErr := s.writeVerackResponse(w, segmentBuf, tlog.VerackStatusInvalidVdiskID)
		if writeErr != nil {
			log.Infof("couldn't write server invalid-vdiskid response: %s", writeErr.Error())
		}
		return
	}

	// version checks out, let's send our verack message back to client
	err = s.writeVerackResponse(w, segmentBuf, tlog.VerackStatusOK)
	if err != nil {
		return
	}

	// return cfg, as connection has been established
	cfg = &ConnectionConfig{
		VdiskID:       vdiskID,
		FirstSequence: verack.FirstSequence(),
	}
	return
}

func (s *Server) writeVerackResponse(w io.Writer, segmentBuf []byte, status tlog.VerAckStatus) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(segmentBuf))
	if err != nil {
		return err
	}

	resp, err := schema.NewRootServerVerAck(seg)
	if err != nil {
		return err
	}

	resp.SetVersion(blockstor.CurrentVersion.UInt32())

	log.Debugf("replying verack with status: %d", status)
	resp.SetStatus(status.Int8())

	return capnp.NewEncoder(w).Encode(msg)
}

func (s *Server) handle(conn *net.TCPConn) error {
	defer conn.Close()
	br := bufio.NewReader(conn)

	cfg, err := s.handshake(br, conn)
	if err != nil {
		err = fmt.Errorf("handshake failed: %s", err.Error())
		log.Info(err)
		return err
	}

	vdisk, created, err := vdiskMgr.get(cfg.VdiskID, s.f, cfg.FirstSequence)
	if err != nil {
		err = fmt.Errorf("couldn't get or create vdisk %s", cfg.VdiskID)
		log.Info(err)
		return err
	}
	if created {
		// start response sender for vdisk
		go s.sendResp(conn, vdisk.vdiskID, vdisk.respChan)
	}

	for {
		// decode
		block, err := s.readDecodeBlock(br)
		if err != nil {
			log.Errorf("failed to decode tlog: %v", err)
			return err
		}

		// check hash
		if err := s.hash(block, vdisk.vdiskID); err != nil {
			log.Debugf("hash check failed:%v\n", err)
			return err
		}

		// store
		vdisk.inputChan <- block
		vdisk.respChan <- &blockResponse{
			Status:    tlog.BlockStatusRecvOK.Int8(),
			Sequences: []uint64{block.Sequence()},
		}
	}
}

func (s *Server) sendResp(conn *net.TCPConn, vdiskID string, respChan chan *blockResponse) {
	segmentBuf := make([]byte, 0, s.maxRespSegmentBufLen)
	for {
		resp := <-respChan
		if err := resp.write(conn, segmentBuf); err != nil {
			log.Infof("failed to send resp to :%v, err:%v", vdiskID, err)
			conn.Close()
			return
		}
	}
}

// read and decode tlog verAck message from client
func (s *Server) readDecodeVerAck(r io.Reader) (*schema.ClientVerAck, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}

	verack, err := schema.ReadRootClientVerAck(msg)
	if err != nil {
		return nil, err
	}

	return &verack, nil
}

// read and decode tlog block message from client
func (s *Server) readDecodeBlock(r io.Reader) (*schema.TlogBlock, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}

	tlb, err := schema.ReadRootTlogBlock(msg)
	if err != nil {
		return nil, err
	}

	return &tlb, nil
}

// hash tlog data and check against given hash from client
func (s *Server) hash(tlb *schema.TlogBlock, vdiskID string) (err error) {
	data, err := tlb.Data()
	if err != nil {
		return
	}

	// get expected hash
	rawHash, err := tlb.Hash()
	if err != nil {
		return
	}
	expectedHash := blockstor.Hash(rawHash)

	// compute hashs based on given data
	hash := blockstor.HashBytes(data)

	if !expectedHash.Equals(hash) {
		err = errors.New("data hash is incorrect")
		return
	}

	return
}
