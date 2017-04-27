package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

type Server struct {
	port    int
	bufSize int
	f       *flusher
}

func NewServer(port int, conf *config) (*Server, error) {
	f, err := newFlusher(conf)
	if err != nil {
		return nil, err
	}

	return &Server{
		port: port,
		f:    f,
	}, nil
}

func (s *Server) Listen() {
	l, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
	if err != nil {
		log.Infof("failed to listen to port %v: %v", s.port, err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Infof("couldn't accept connection: %v", err)
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) error {

	defer conn.Close()
	for {
		data, err := s.readData(conn)
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

		volID, err := tlb.VolumeId()
		if err != nil {
			log.Debugf("failed to get volume ID: %v", err)
			return err
		}

		// check hash
		if err := s.hash(tlb, volID); err != nil {
			log.Debugf("hash check failed:%v\n", err)
			return err
		}

		// store
		resp := s.f.store(tlb, volID)

		if err := resp.write(conn); err != nil {
			return err
		}
	}
}
func (s *Server) readData(conn net.Conn) ([]byte, error) {
	// read length prefix
	// as described in https://capnproto.org/encoding.html#serialization-over-a-stream
	var segmentNum, length uint32

	if err := binary.Read(conn, binary.LittleEndian, &segmentNum); err != nil {
		return nil, err
	}

	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length*8)
	_, err := io.ReadFull(conn, data)
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
func (s *Server) hash(tlb *schema.TlogBlock, volID string) (err error) {
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
