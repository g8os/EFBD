package tlogclient

import (
	"bufio"
	"net"

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
	if err := c.encodeCapnp(op, seq, hash[:], lba, timestamp, data, size); err != nil {
		return err
	}
	return c.bw.Flush()
}
