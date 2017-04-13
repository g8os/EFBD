package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"strconv"

	"zombiezen.com/go/capnproto2"
)

type Server struct {
	port    int
	bufSize int
	f       *flusher
}

func NewServer(port int, conf *config) (*Server, error) {
	return &Server{
		port:    port,
		bufSize: conf.bufSize,
		f:       newFlusher(conf),
	}, nil
}

func (s *Server) Listen() {
	l, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
	if err != nil {
		log.Fatalf("[ERROR] failed to listen to port:%v err:%v", s.port, err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("[ERROR] accepting connection:%v\n", err)
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) error {
	defer conn.Close()
	for {
		// read
		buf := make([]byte, s.bufSize)
		_, err := io.ReadFull(conn, buf)
		if err != nil {
			return err
		}

		// decode
		tlb, err := s.decode(bytes.NewBuffer(buf))
		if err != nil {
			return err
		}
		log.Printf("sequence = %v\n", tlb.Sequence())
		// check crc

		// store packet
		s.f.store(tlb)

		// check if we can do flush
		if err := s.f.checkDoFlush(tlb.VolumeId()); err != nil {
			log.Printf("[ERROR] flush : %v\n", err)
		}
	}
}

func (s *Server) decode(r io.Reader) (*TlogBlock, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}
	tlb, err := ReadRootTlogBlock(msg)
	if err != nil {
		return nil, err
	}
	return &tlb, nil
}
