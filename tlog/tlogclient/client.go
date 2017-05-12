package tlogclient

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"time"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/tlog"
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

// Result defines a struct that contains
// response and error from tlog.
type Result struct {
	Resp *Response
	Err  error
}

// Client defines a Tlog Client.
// This client is not thread/goroutine safe
type Client struct {
	addr            string
	vdiskID         string
	conn            *net.TCPConn
	bw              *bufio.Writer
	capnpSegmentBuf []byte
}

// New creates a new tlog client
func New(addr string, vdiskID string) (*Client, error) {
	conn, err := createConn(addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:    addr,
		vdiskID: vdiskID,
		conn:    conn,
		bw:      bufio.NewWriter(conn),
	}, nil
}

// Recv get channel of responses and errors (Result)
func (c *Client) Recv(chanSize int) <-chan *Result {
	reChan := make(chan *Result, chanSize)
	go func() {
		for {
			tr, err := c.RecvOne()
			reChan <- &Result{
				Resp: tr,
				Err:  err,
			}
		}
	}()
	return reChan
}

// RecvOne receive one response
func (c *Client) RecvOne() (*Response, error) {
	// read prefix to get the length
	_, length, err := tlog.ReadCapnpPrefix(c.conn)
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
func (c *Client) Send(op uint8, seq uint64, lba, timestamp uint64,
	data []byte, size uint64) error {

	hash := blockstor.HashBytes(data)

	b, err := c.buildCapnp(op, seq, hash[:], lba, timestamp, data, size)
	if err != nil {
		return err
	}

	// adjust capnp segment buffer and tcp write buffer
	if len(b) > cap(c.capnpSegmentBuf) {
		c.conn.SetWriteBuffer(len(b))
		c.capnpSegmentBuf = make([]byte, 0, len(b))
	}

	// add capnp prefix
	buf := new(bytes.Buffer)
	if err := tlog.WriteCapnpPrefix(buf, len(b)); err != nil {
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
