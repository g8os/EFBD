package tlogclient

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/g8os/blockstor"
)

var (
	// ErrClosed happens when tlog server-client connection is closed.
	ErrClosed = errors.New("connection closed")
)

// Response defines a response from tlog server
type Response struct {
	Status    int8     // status of the call, negative means failed
	Sequences []uint64 // flushed sequences number (optional)
}

// Client defines a Tlog Client.
// This client is not thread/goroutine safe
type Client struct {
	addr string
	conn *net.TCPConn
	bw   *bufio.Writer
}

// New creates a new tlog client
func New(addr string) (*Client, error) {
	conn, err := createConn(addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		addr: addr,
		conn: conn,
		bw:   bufio.NewWriter(conn),
	}, nil
}

// Recv get channel of responses and errors
func (c *Client) Recv() (<-chan *Response, <-chan error) {
	respChan := make(chan *Response)
	errChan := make(chan error)

	go func() {
		for {
			tr, err := c.RecvOne()
			if err != nil {
				errChan <- err
				continue
			}
			respChan <- tr
		}
	}()
	return respChan, errChan
}

// RecvOne receive one response
func (c *Client) RecvOne() (*Response, error) {
	// read prefix to get the length
	length, err := c.recvPrefix()
	if err != nil {
		return nil, err
	}

	// read actual data
	data := make([]byte, length)
	_, err = io.ReadFull(c.conn, data)
	if err != nil {
		return nil, err
	}

	// decode capnp and build response
	tr, err := decodeResponse(data)
	if err != nil {
		return nil, err
	}

	capSeqs, err := tr.Sequences()
	if err != nil {
		return nil, err
	}
	seqs := []uint64{}
	for i := 0; i < capSeqs.Len(); i++ {
		seqs = append(seqs, capSeqs.At(i))
	}
	return &Response{
		Status:    tr.Status(),
		Sequences: seqs,
	}, nil
}

func (c *Client) recvPrefix() (int, error) {
	// check if prefix already fully retrieved
	endPrefix := func(data []byte) bool {
		if len(data) < 3 {
			return false
		}
		return strings.HasSuffix(string(data), "\r\n")
	}

	// min prefix len = 3
	prefix := make([]byte, 3)
	_, err := c.conn.Read(prefix)
	if err != nil {
		return 0, err
	}

	// read byte by byte
	for !endPrefix(prefix) {
		if len(prefix) > 6 { // prefix too long
			return 0, fmt.Errorf("prefix too long")
		}

		temp := make([]byte, 1)
		if _, err := c.conn.Read(temp); err != nil {
			return 0, err
		}
		prefix = append(prefix, temp...)
	}
	return strconv.Atoi(string(prefix[:len(prefix)-2]))
}

func createConn(addr string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	conn.SetKeepAlive(true)
	return conn, nil
}

// Send sends the transaction tlog to server.
// It returns error in these cases:
// - broken network
// - failed to send all tlog
// in case of errors, client is not in valid state,
// shouldn't be used anymore
func (c *Client) Send(vdiskID string, seq uint64, lba, timestamp uint64, data []byte) error {
	hash := blockstor.HashBytes(data)

	b, err := buildCapnp(vdiskID, seq, hash[:], lba, timestamp, data)
	if err != nil {
		return err
	}

	// build capnp prefix as described at https://capnproto.org/encoding.html#serialization-over-a-stream
	buf := new(bytes.Buffer)

	var prefix uint32
	if err := binary.Write(buf, binary.LittleEndian, prefix); err != nil {
		return err
	}

	prefix = uint32(len(b) / 8)
	if err := binary.Write(buf, binary.LittleEndian, prefix); err != nil {
		return err
	}

	_, err = c.sendAll(append(buf.Bytes(), b...))
	return err
}

func (c *Client) sendAll(b []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	nWrite := 0
	for nWrite < len(b) {
		n, err := c.bw.Write(b[nWrite:])
		if err != nil && !isNetTempErr(err) {
			return nWrite, err
		}
		if n == 0 {
			return nWrite, ErrClosed
		}
		if err := c.bw.Flush(); !isNetTempErr(err) {
			return nWrite, err
		}
		nWrite += n
	}
	return nWrite, nil
}

func isNetTempErr(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		return true
	}
	return false
}
