package tlogclient

import (
	"bufio"
	"net"
	"time"

	"github.com/g8os/blockstor"
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
func New(addr, vdiskID string) (*Client, error) {
	c := &Client{
		addr:    addr,
		vdiskID: vdiskID,
	}
	if err := c.createConn(); err != nil {
		return nil, err
	}

	return c, nil
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
	// decode capnp and build response
	tr, err := c.decodeResponse()
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

func (c *Client) createConn() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}

	conn.SetKeepAlive(true)
	c.conn = conn
	c.bw = bufio.NewWriter(conn)
	return nil
}

// Send sends the transaction tlog to server.
// It returns error in these cases:
// - failed to encode the capnp.
// - failed to recover from broken network connection.
func (c *Client) Send(op uint8, seq, lba, timestamp uint64,
	data []byte, size uint64) error {

	var err error

	hash := blockstor.HashBytes(data)

	send := func() error {
		if err = c.encodeCapnp(op, seq, hash[:], lba, timestamp, data, size); err != nil {
			return err
		}
		return c.bw.Flush()
	}

	okToSend := true

	for i := 0; i < sendRetryNum+1; i++ {
		if okToSend {
			if err = send(); err == nil {
				return nil
			}

			if _, ok := err.(net.Error); !ok {
				return err // no need to rety if it is not network error.
			}
		}

		// First sleep = 0 second,
		// so we don't need to sleep in case of simple closed connection.
		// We sleep in next iteration because there might be something error in
		// the network connection or the tlog server that need time to be recovered.
		time.Sleep(time.Duration(i) * time.Duration(sendSleepMs) * time.Millisecond)

		if err = c.createConn(); err != nil {
			okToSend = false
		} else {
			okToSend = true
		}
	}
	return err
}

const (
	sendRetryNum = 3
	sendSleepMs  = 500
)
