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

// Server defines a tlog server
type Server struct {
	port                 int
	bufSize              int
	maxRespSegmentBufLen int // max len of response capnp segment buffer
	poolFactory          tlog.RedisPoolFactory
	listener             net.Listener
	flusherConf          *flusherConfig
	vdiskMgr             *vdiskManager
}

// NewServer creates a new tlog server
func NewServer(conf *Config, poolFactory tlog.RedisPoolFactory) (*Server, error) {
	if conf == nil {
		return nil, errors.New("tlogserver requires a non-nil config")
	}
	if poolFactory == nil {
		return nil, errors.New("tlogserver requires a non-nil RedisPoolFactory")
	}

	var err error
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
		log.Infof("Started listening on local address %s", listener.Addr().String())
	}

	// used to created a flusher on rumtime
	flusherConf := &flusherConfig{
		K:         conf.K,
		M:         conf.M,
		FlushSize: conf.FlushSize,
		FlushTime: conf.FlushTime,
		PrivKey:   conf.PrivKey,
		HexNonce:  conf.HexNonce,
	}

	return &Server{
		poolFactory:          poolFactory,
		listener:             listener,
		flusherConf:          flusherConf,
		maxRespSegmentBufLen: schema.RawTlogRespLen(conf.FlushSize),
		vdiskMgr:             newVdiskManager(conf.BlockSize, conf.FlushSize),
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
		go func() {
			defer func() {
				// recover from handle panics,
				// to keep server up and running at all costs
				if r := recover(); r != nil {
					log.Error("connection dropped because of an internal panic: ", r)
				}
			}()

			addr := conn.RemoteAddr()

			err := s.handle(tcpConn)
			if err == nil {
				log.Infof("connection from %s dropped", addr.String())
			} else {
				log.Errorf("connection from %s dropped with an error: %s", addr.String(), err.Error())
			}
		}()
	}
}

// ListenAddr returns the address the (tcp) server is listening on
func (s *Server) ListenAddr() string {
	return s.listener.Addr().String()
}

// handshake stage, required prior to receiving blocks
func (s *Server) handshake(r io.Reader, w io.Writer, conn *net.TCPConn) (vd *vdisk, err error) {
	status := tlog.HandshakeStatusInternalServerError

	// always return response, even in case of a panic,
	// but normally this is triggered because of a(n early) return
	defer func() {
		segmentBuf := make([]byte, 0, schema.RawServerHandshakeLen())
		err := s.writeHandshakeResponse(w, segmentBuf, status)
		if err != nil {
			log.Infof("couldn't write server %s response: %s",
				status.String(), err.Error())
		}
	}()

	req, err := s.readDecodeHandshakeRequest(r)
	if err != nil {
		status = tlog.HandshakeStatusInvalidRequest
		err = fmt.Errorf("couldn't decode client HandshakeReq: %s", err.Error())
		return
	}

	log.Debug("received handshake request from incoming connection")

	// get the version from the handshake req and validate it
	clientVersion := blockstor.Version(req.Version())
	if clientVersion.Compare(tlog.MinSupportedVersion) < 0 {
		status = tlog.HandshakeStatusInvalidVersion
		err = fmt.Errorf("client version (%s) is not supported by this server", clientVersion)
		return // error return
	}

	log.Debug("incoming connection checks out with version: ", clientVersion)

	// get the vdiskID from the handshake req
	vdiskID, err := req.VdiskID()
	if err != nil {
		status = tlog.HandshakeStatusInvalidVdiskID
		err = fmt.Errorf("couldn't get vdiskID from handshakeReq: %s", err.Error())
		return // error return
	}

	vd, err = s.vdiskMgr.Get(vdiskID, req.FirstSequence(), s.createFlusher, conn)
	if err != nil {
		status = tlog.HandshakeStatusInternalServerError
		err = fmt.Errorf("couldn't create vdisk %s: %s", vdiskID, err.Error())
		return
	}

	if req.ResetFirstSequence() {
		if err = vd.resetFirstSequence(req.FirstSequence(), conn); err != nil {
			status = tlog.HandshakeStatusInternalServerError
			err = fmt.Errorf("couldn't reset vdisk first sequence %s: %s", vdiskID, err.Error())
			return
		}
	}

	log.Debug("handshake phase successfully completed")
	status = tlog.HandshakeStatusOK
	return // success return
}

func (s *Server) createFlusher(vdiskID string) (*flusher, error) {
	redisPool, err := s.poolFactory.NewRedisPool(vdiskID)
	if err != nil {
		return nil, err
	}

	return newFlusher(s.flusherConf, redisPool)
}

func (s *Server) writeHandshakeResponse(w io.Writer, segmentBuf []byte, status tlog.HandshakeStatus) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(segmentBuf))
	if err != nil {
		return err
	}

	resp, err := schema.NewRootHandshakeResponse(seg)
	if err != nil {
		return err
	}

	resp.SetVersion(blockstor.CurrentVersion.UInt32())

	log.Debug("replying handshake with status: ", status)
	resp.SetStatus(status.Int8())

	return capnp.NewEncoder(w).Encode(msg)
}

func (s *Server) handle(conn *net.TCPConn) error {
	defer conn.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	br := bufio.NewReader(conn)

	vdisk, err := s.handshake(br, conn, conn)
	if err != nil {
		err = fmt.Errorf("handshake failed: %s", err.Error())
		return err
	}
	defer vdisk.removeClient(conn)
	go s.sendResp(ctx, conn, vdisk.ID(), vdisk.ResponseChan())

	for {
		msgType, err := tlog.ReadCheckMessageType(br)
		if err != nil {
			if err == io.EOF { // EOF in this stage is not an error
				err = nil
			}

			return err
		}

		switch msgType {
		case tlog.MessageForceFlush, tlog.MessageForceFlushAtSeq:
			err = s.handleForceFlush(vdisk, br, msgType)
		case tlog.MessageTlogBlock:
			err = s.handleBlock(vdisk, br)
		default:
			err = fmt.Errorf("unhandled message type:%v", msgType)
		}
		if err != nil {
			return err
		}

	}
}

func (s *Server) handleForceFlush(vd *vdisk, br *bufio.Reader, mType uint8) error {
	if mType == tlog.MessageForceFlush {
		vd.forceFlush()
	} else {
		msg, err := capnp.NewDecoder(br).Decode()
		if err != nil {
			return err
		}

		cmd, err := schema.ReadRootCommand(msg)
		if err != nil {
			return err
		}

		vd.forceFlushAtSeq(cmd.Sequence())
	}

	vd.respChan <- &BlockResponse{
		Status:    tlog.BlockStatusForceFlushReceived.Int8(),
		Sequences: []uint64{vd.lastSeqFlushed},
	}
	return nil
}

func (s *Server) handleBlock(vd *vdisk, br *bufio.Reader) error {
	// decode
	block, err := s.ReadDecodeBlock(br)
	if err != nil {
		log.Errorf("failed to decode tlog: %v", err)
		return err
	}

	// check hash
	if err := s.hash(block, vd.vdiskID); err != nil {
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

func (s *Server) sendResp(ctx context.Context, w io.Writer, vdiskID string, respChan <-chan *BlockResponse) {
	segmentBuf := make([]byte, 0, s.maxRespSegmentBufLen)
	for {
		select {
		case resp := <-respChan:
			if err := resp.Write(w, segmentBuf); err != nil {
				log.Infof("failed to send resp to :%v, err:%v", vdiskID, err)
				return
			}
		case <-ctx.Done():
			log.Debugf("abort current sendResp goroutine for vdisk:%v", vdiskID)
			return
		}
	}
}

// read and decode tlog handshake request message from client
func (s *Server) readDecodeHandshakeRequest(r io.Reader) (*schema.HandshakeRequest, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}

	resp, err := schema.ReadRootHandshakeRequest(msg)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ReadDecodeBlock reads and decodes tlog block message from client
func (s *Server) ReadDecodeBlock(r io.Reader) (*schema.TlogBlock, error) {
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
