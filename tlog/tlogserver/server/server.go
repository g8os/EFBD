package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

// Server defines a tlog server
type Server struct {
	port                 int
	acceptAddr           string
	bufSize              int
	maxRespSegmentBufLen int // max len of response capnp segment buffer
	listener             net.Listener
	coordListener        net.Listener
	waitConnectAddr      string
	flusherConf          *flusherConfig
	vdiskMgr             *vdiskManager
	ctx                  context.Context
}

// NewServer creates a new tlog server
func NewServer(conf *Config, configSource config.Source) (*Server, error) {
	if conf == nil {
		return nil, errors.New("tlogserver requires a non-nil config")
	}

	var (
		err                     error
		coordListener, listener net.Listener
	)

	// tlog main listen addr
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

	// tlog coord listen address
	if conf.WaitListenAddr != "" {
		if conf.WaitListenAddr == WaitListenAddrRandom {
			conf.WaitListenAddr = "127.0.0.1:0"
		}
		coordListener, err = net.Listen("tcp", conf.WaitListenAddr)
		if err != nil {
			return nil, err
		}
	}

	// used to created a flusher on rumtime
	flusherConf := &flusherConfig{
		DataShards:   conf.DataShards,
		ParityShards: conf.ParityShards,
		FlushSize:    conf.FlushSize,
		FlushTime:    conf.FlushTime,
		PrivKey:      conf.PrivKey,
	}

	vdiskManager := newVdiskManager(conf.AggMq, conf.FlushSize, configSource)
	return &Server{
		listener:             listener,
		acceptAddr:           conf.AcceptAddr,
		coordListener:        coordListener,
		waitConnectAddr:      conf.WaitConnectAddr,
		flusherConf:          flusherConf,
		maxRespSegmentBufLen: schema.RawTlogRespLen(conf.FlushSize),
		vdiskMgr:             vdiskManager,
	}, nil
}

// Listen to incoming (tcp) Requests
func (s *Server) Listen(ctx context.Context) {
	s.ctx = ctx
	defer s.listener.Close()

	if s.coordListener != nil {
		log.Infof("s.coordListener listen at: %v", s.coordListener.Addr().String())
		go func() {
			for {
				// start listener for the coordination (wait-other-tlog) purposes
				conn, err := s.coordListener.Accept()
				if err != nil {
					log.Errorf("couldn't accept coord connection:%v", err)
					continue
				}

				log.Infof("accepted coordination connection from %s", conn.RemoteAddr().String())

				go func(conn net.Conn) {
					// start handler for tlog waiter
					defer func() {
						conn.Close()
					}()
					for {
						select {
						case <-ctx.Done():
							return
						default:
							b := make([]byte, 1)
							conn.SetReadDeadline(time.Now().Add(1 * time.Second))
							_, err := conn.Read(b)
							if err != nil {
								nerr, isNetErr := err.(net.Error)
								if isNetErr && nerr.Timeout() {
									continue
								}
								return
							}
						}
					}
				}(conn)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				log.Infof("couldn't accept connection: %v", err)
				continue
			}

			remoteAddr := conn.RemoteAddr().String()
			log.Infof("connection request from %s", remoteAddr)
			host, _, err := net.SplitHostPort(remoteAddr)
			if err != nil {
				log.Errorf("fail to parse remote address: %v", err)
				conn.Close()
				continue
			}

			if s.acceptAddr != "" && s.acceptAddr != host {
				log.Infof("connection from %s refused, it does not match the accept address configured %s", remoteAddr, s.acceptAddr)
				conn.Close()
				continue
			}
			log.Infof("connection accepted from %s", remoteAddr)

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

func (s *Server) WaitListenAddr() string {
	return s.coordListener.Addr().String()
}

// handshake stage, required prior to receiving blocks
func (s *Server) handshake(r io.Reader, w io.Writer, conn *net.TCPConn) (vd *vdisk, err error) {
	status := tlog.HandshakeStatusInternalServerError
	var lastSeq uint64
	var vdiskReady bool

	// always return response, even in case of a panic,
	// but normally this is triggered because of a(n early) return
	defer func() {
		segmentBuf := make([]byte, 0, schema.RawServerHandshakeLen())
		err := s.writeHandshakeResponse(w, segmentBuf, status, lastSeq, !vdiskReady)
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
	clientVersion := zerodisk.VersionFromUInt32(req.Version())
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

	log.Infof("get vdisk %v", vdiskID)
	vd, err = s.vdiskMgr.Get(s.ctx, vdiskID, conn, s.flusherConf, s.waitConnectAddr)
	if err != nil {
		status = tlog.HandshakeStatusInternalServerError
		err = fmt.Errorf("couldn't create vdisk %s: %s", vdiskID, err.Error())
		return
	}
	vdiskReady = vd.Ready()

	lastSeq, err = vd.connect(conn)
	if err != nil {
		status = tlog.HandshakeStatusInternalServerError
		err = fmt.Errorf("couldn't connect to vdisk %s: %s", vdiskID, err.Error())
		return
	}

	log.Debug("handshake phase successfully completed")
	status = tlog.HandshakeStatusOK
	return // success return
}

func (s *Server) writeHandshakeResponse(w io.Writer, segmentBuf []byte, status tlog.HandshakeStatus,
	lastFlushedSeq uint64, waitTlogReady bool) error {
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
	resp.SetWaitTlogReady(waitTlogReady)

	log.Debug("replying handshake with status: ", status)
	resp.SetStatus(status.Int8())

	return capnp.NewEncoder(w).Encode(msg)
}

func (s *Server) handle(conn *net.TCPConn) error {

	br := bufio.NewReader(conn)

	vd, err := s.handshake(br, conn, conn)
	if err != nil {
		err = fmt.Errorf("handshake failed: %s", err.Error())
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
