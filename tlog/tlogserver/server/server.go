package server

import (
	"bufio"
	"context"
	"io"
	"net"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
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
	listener             net.Listener
	flusherConf          *flusherConfig
	vdiskMgr             *vdiskManager
	ctx                  context.Context
}

// NewServer creates a new tlog server
func NewServer(conf *Config, configSource config.Source) (*Server, error) {
	if conf == nil {
		return nil, errors.New("tlogserver requires a non-nil config")
	}

	var err error
	var listener net.Listener

	if conf.ListenAddr != "" {
		// listen for tcp requests on given address
		listener, err = net.Listen("tcp", conf.ListenAddr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to listen to %v", conf.ListenAddr)
		}
	} else {
		// listen for tcp requests on localhost using any available port
		listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			if listener, err = net.Listen("tcp6", "[::1]:0"); err != nil {
				return nil, errors.Wrap(err, "failed to listen on localhost, port")
			}
		}
		log.Infof("Started listening on local address %s", listener.Addr().String())
	}

	// used to created a flusher on rumtime
	flusherConf := &flusherConfig{
		FlushSize: conf.FlushSize,
		FlushTime: conf.FlushTime,
		PrivKey:   conf.PrivKey,
	}

	vdiskManager := newVdiskManager(conf.AggMq, conf.FlushSize, configSource)
	return &Server{
		listener:             listener,
		flusherConf:          flusherConf,
		maxRespSegmentBufLen: schema.RawTlogRespLen(conf.FlushSize),
		vdiskMgr:             vdiskManager,
	}, nil
}

// Listen to incoming (tcp) Requests
func (s *Server) Listen(ctx context.Context) {
	s.ctx = ctx
	defer s.listener.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
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
}

// ListenAddr returns the address the (tcp) server is listening on
func (s *Server) ListenAddr() string {
	return s.listener.Addr().String()
}

// handshake stage, required prior to receiving blocks
func (s *Server) handshake(r io.Reader, w io.Writer, conn *net.TCPConn) (vd *vdisk, err error) {
	status := tlog.HandshakeStatusInternalServerError
	var lastSeq uint64

	// always return response, even in case of a panic,
	// but normally this is triggered because of a(n early) return
	defer func() {
		segmentBuf := make([]byte, 0, schema.RawServerHandshakeLen())
		err := s.writeHandshakeResponse(w, segmentBuf, status, lastSeq)
		if err != nil {
			log.Infof("couldn't write server %s response: %s",
				status.String(), err.Error())
		}
	}()

	req, err := s.readDecodeHandshakeRequest(r)
	if err != nil {
		status = tlog.HandshakeStatusInvalidRequest
		err = errors.Wrap(err, "couldn't decode client HandshakeReq")
		return
	}

	log.Debug("received handshake request from incoming connection")

	// get the version from the handshake req and validate it
	clientVersion := zerodisk.VersionFromUInt32(req.Version())
	if clientVersion.Compare(tlog.MinSupportedVersion) < 0 {
		status = tlog.HandshakeStatusInvalidVersion
		err = errors.Newf("client version (%s) is not supported by this server", clientVersion)
		return // error return
	}

	log.Debug("incoming connection checks out with version: ", clientVersion)

	// get the vdiskID from the handshake req
	vdiskID, err := req.VdiskID()
	if err != nil {
		status = tlog.HandshakeStatusInvalidVdiskID
		err = errors.Wrap(err, "couldn't get vdiskID from handshakeReq")
		return // error return
	}

	vd, err = s.vdiskMgr.Get(s.ctx, vdiskID, conn, s.flusherConf)
	if err != nil {
		status = tlog.HandshakeStatusInternalServerError
		err = errors.Wrapf(err, "couldn't create vdisk %s", vdiskID)
		return
	}

	lastSeq, err = vd.connect(conn)
	if err != nil {
		status = tlog.HandshakeStatusInternalServerError
		err = errors.Wrapf(err, "couldn't connect to vdisk %s", vdiskID)
		return
	}

	log.Debug("handshake phase successfully completed")
	status = tlog.HandshakeStatusOK
	return // success return
}

func (s *Server) writeHandshakeResponse(w io.Writer, segmentBuf []byte, status tlog.HandshakeStatus,
	lastFlushedSeq uint64) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(segmentBuf))
	if err != nil {
		return err
	}

	resp, err := schema.NewRootHandshakeResponse(seg)
	if err != nil {
		return err
	}

	resp.SetVersion(zerodisk.CurrentVersion.UInt32())
	resp.SetLastFlushedSequence(lastFlushedSeq)

	log.Debug("replying handshake with status: ", status)
	resp.SetStatus(status.Int8())

	return capnp.NewEncoder(w).Encode(msg)
}

func (s *Server) handle(conn *net.TCPConn) error {

	br := bufio.NewReader(conn)

	vd, err := s.handshake(br, conn, conn)
	if err != nil {
		err = errors.Wrap(err, "handshake failed")
		conn.Close()
		return err
	}
	return vd.handle(conn, br, s.maxRespSegmentBufLen)
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

// ReadDecodeClientMessage reads and decodes tlog message from client
func (s *Server) ReadDecodeClientMessage(r io.Reader) (*schema.TlogClientMessage, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}

	cmd, err := schema.ReadRootTlogClientMessage(msg)
	if err != nil {
		return nil, err
	}

	return &cmd, nil
}
