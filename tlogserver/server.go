package main

// #include <isa-l/crc.h>
// #cgo LDFLAGS: -lisal
import "C"
import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"unsafe"

	"github.com/g8os/tlog/client"
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
		data := make([]byte, s.bufSize)
		_, err := io.ReadFull(conn, data)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(data)

		// decode
		tlb, err := s.decode(buf)
		if err != nil {
			log.Printf("failed to decode tlog:%v\n", err)
			return err
		}

		volID, err := tlb.VolumeId()
		if err != nil {
			log.Printf("[ERROR] failed to get volume ID:%v", err)
			return err
		}

		// check crc
		if err := s.crc(tlb, volID); err != nil {
			log.Printf("[ERROR] crc:%v\n", err)
			return err
		}

		// store
		resp := s.f.store(tlb, volID)

		if err := resp.write(conn); err != nil {
			return err
		}
	}
}

// decode tlog message from client
func (s *Server) decode(r io.Reader) (*client.TlogBlock, error) {
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}

	tlb, err := client.ReadRootTlogBlock(msg)
	if err != nil {
		return nil, err
	}

	return &tlb, nil
}

func (s *Server) crc(tlb *client.TlogBlock, volID string) error {
	data, err := tlb.Data()
	if err != nil {
		return err
	}

	crc := C.crc32_ieee(0, (*C.uchar)(unsafe.Pointer(&data[0])), (C.uint64_t)(len(data)))

	if uint32(crc) != tlb.Crc32() {
		return fmt.Errorf("invalid crc for tlogs %v from %v", tlb.Sequence(), volID)
	}
	return nil
}
