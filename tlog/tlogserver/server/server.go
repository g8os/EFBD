package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor"
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
		go s.handle(conn)
	}
}

// ListenAddr returns the address the (tcp) server is listening on
func (s *Server) ListenAddr() string {
	return s.listener.Addr().String()
}

func (s *Server) handle(conn net.Conn) error {
	var vd *vdisk

	br := bufio.NewReader(conn)
	defer conn.Close()
	for {
		data, err := s.readData(br)
		if err != nil {
			return err
		}

		buf := bytes.NewBuffer(data)

		// decode
		tlb, err := s.decode(buf)
		if err != nil {
			log.Debugf("failed to decode tlog: %v", err)
			return err
		}

		// vdiskID
		curVdiskID, err := tlb.VdiskID()
		if err != nil {
			log.Infof("failed to get vdisk ID: %v", err)
			return err
		}

		if vd == nil {
			vd, err = vdiskMgr.get(curVdiskID, s.f)
			if err != nil {
				log.Infof("failed to vdisk: %v, err: %v", curVdiskID, err)
				return err
			}
			go s.sendResp(conn, vd.vdiskID, vd.respChan)
		}

		if vd.vdiskID != curVdiskID {
			err = fmt.Errorf("invalid vdiskID. expected: %v, got: %v", vd.vdiskID, curVdiskID)
			log.Info(err)
			return err
		}

		// check hash
		if err := s.hash(tlb, vd.vdiskID); err != nil {
			log.Debugf("hash check failed:%v\n", err)
			return err
		}

		// store
		vd.inputChan <- tlb
		vd.respChan <- &response{
			Status:    tlog.StatusBlockRecvOK,
			Sequences: []uint64{tlb.Sequence()},
		}
	}
}

func (s *Server) sendResp(conn net.Conn, vdiskID string, respChan chan *response) {
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
func (s *Server) readData(rd io.Reader) ([]byte, error) {
	// read length prefix
	_, length, err := tlog.ReadCapnpPrefix(rd)
	if err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(rd, data)
	return data, err
}

// decode tlog message from client
func (s *Server) decode(r io.Reader) (*schema.TlogBlock, error) {
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
