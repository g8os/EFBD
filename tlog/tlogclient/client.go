package tlogclient

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
)

// Response defines a response from tlog server
type Response struct {
	Status    tlog.BlockStatus // status of the call
	Sequences []uint64         // flushed sequences number (optional)
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

// New creates a new tlog client.
// The client is not goroutine safe.
func New(addr, vdiskID string, firstSequence uint64) (*Client, error) {
	client := &Client{
		addr:    addr,
		vdiskID: vdiskID,
	}
	err := client.createConn()
	if err != nil {
		return nil, fmt.Errorf("client couldn't be created: %s", err.Error())
	}

	err = client.handshake(firstSequence)
	if err != nil {
		if err := client.Close(); err != nil {
			log.Debug("couldn't close open connection of invalid client:", err)
		}
		return nil, fmt.Errorf("client handshake failed: %s", err.Error())
	}

	return client, nil
}

func (c *Client) handshake(firstSequence uint64) error {
	// send handshake request
	err := c.encodeHandshakeCapnp(firstSequence)
	if err != nil {
		return err
	}
	err = c.bw.Flush()
	if err != nil {
		return err
	}

	// receive handshake response
	resp, err := c.decodeHandshakeResponse()
	if err != nil {
		return err
	}

	// check server response status
	err = tlog.HandshakeStatus(resp.Status()).Error()
	if err != nil {
		return err
	}

	// all checks out, ready to go!
	return nil
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
	tr, err := c.decodeBlockResponse()
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
		Status:    tlog.BlockStatus(tr.Status()),
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
		err = c.encodeBlockCapnp(op, seq, hash[:], lba, timestamp, data, size)
		if err != nil {
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
		time.Sleep(time.Duration(i) * sendSleepTime)

		if err = c.createConn(); err != nil {
			okToSend = false
		} else {
			okToSend = true
		}
	}
	return err
}

// Close the open connection, making this client invalid
func (c *Client) Close() error {
	return c.conn.Close()
}

const (
	sendRetryNum  = 3
	sendSleepTime = 500 * time.Millisecond
)
