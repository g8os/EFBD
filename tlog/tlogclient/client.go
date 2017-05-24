package tlogclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient/blockbuffer"
)

const (
	sendRetryNum     = 5
	sendSleepTime    = 500 * time.Millisecond // sleep duration before retrying the Send
	resendTimeoutDur = 5 * time.Second        // duration to wait before re-send the tlog.
	readTimeout      = 5 * time.Second
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
// This client is not thread/goroutine safe.
type Client struct {
	addr            string
	vdiskID         string
	conn            *net.TCPConn
	bw              writerFlusher
	rd              io.Reader // reader of this client
	blockBuffer     *blockbuffer.Buffer
	capnpSegmentBuf []byte

	// write lock, to protect against parallel Send
	// which is not goroutine safe yet
	wLock sync.Mutex

	// read lock, to protect it from race condition
	// caused by 'handshake' and recvOne
	rLock      sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// New creates a new tlog client for a vdisk with 'addr' is the tlogserver address.
// 'firstSequence' is the first sequence number this client is going to send.
// The client is not goroutine safe.
func New(addr, vdiskID string, firstSequence uint64) (*Client, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := &Client{
		addr:        addr,
		vdiskID:     vdiskID,
		blockBuffer: blockbuffer.NewBuffer(resendTimeoutDur),
		ctx:         ctx,
		cancelFunc:  cancelFunc,
	}
	if err := client.connect(firstSequence); err != nil {
		return nil, err
	}

	go client.resender()
	return client, nil
}

func (c *Client) connect(firstSequence uint64) error {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	err := c.createConn()
	if err != nil {
		return fmt.Errorf("client couldn't be created: %s", err.Error())
	}

	err = c.handshake(firstSequence)
	if err != nil {
		if err := c.conn.Close(); err != nil {
			log.Debug("couldn't close open connection of invalid client:", err)
		}
		return fmt.Errorf("client handshake failed: %s", err.Error())
	}
	return nil
}

// goroutine which re-send the block.
func (c *Client) resender() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case block := <-c.blockBuffer.TimedOut():
			data, err := block.Data()
			if err != nil {
				log.Errorf("client resender failed to get data block:%v", err)
				continue
			}

			err = c.Send(block.Operation(), block.Sequence(), block.Lba(), block.Timestamp(), data, block.Size())
			if err != nil {
				log.Errorf("client resender failed to send data:%v", err)
			}
		}
	}
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
func (c *Client) Recv() <-chan *Result {
	// response channel
	// three is big enough size:
	// - one waiting to be fetched by application/user
	// - one for the response we just received
	// - one for (hopefully) optimization, so we can read from network
	//	 before needed and then put it to this channel.
	reChan := make(chan *Result, 3)

	go func() {
		for {
			tr, err := c.recvOne()

			// it is timeout error
			// client can't really read something
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				continue
			}

			if tr != nil && len(tr.Sequences) > 0 {
				// if it successfully received by server, delete from buffer
				if tr.Status == tlog.BlockStatusRecvOK {
					c.blockBuffer.Delete(tr.Sequences[0])
				}

				/* enclose this code part with comment because we currently doesn't
				  have this case in server.
				// if it failed to be received, promote it to be
				// timed out as soon as possible.
				if status == tlog.BlockStatusRecvFailed {
					if err := c.blockBuffer.Promote(seq); err != nil {
						// failed to promote, forward the server's response to user.
						log.Infof("tlog client failed to promote %v, err: %v", seq, err)
					} else {
						// successfully promoted, ignore the response.
						// we will resend it
						continue
					}
				}
				*/
			}

			reChan <- &Result{
				Resp: tr,
				Err:  err,
			}
		}
	}()
	return reChan
}

// recvOne receive one response
func (c *Client) recvOne() (*Response, error) {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	// set read deadline, so we don't have deadlock
	// for rLock
	c.conn.SetReadDeadline(time.Now().Add(readTimeout))

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
	c.rd = conn
	return nil
}

// Send sends the transaction tlog to server.
// It returns error in these cases:
// - failed to encode the capnp.
// - failed to recover from broken network connection.
func (c *Client) Send(op uint8, seq, lba, timestamp uint64,
	data []byte, size uint64) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	block, err := c.send(op, seq, lba, timestamp, data, size)
	if err == nil && block != nil {
		c.blockBuffer.Add(block)
	}
	return err
}

func (c *Client) send(op uint8, seq, lba, timestamp uint64,
	data []byte, size uint64) (block *schema.TlogBlock, err error) {

	hash := blockstor.HashBytes(data)

	send := func() (*schema.TlogBlock, error) {
		block, err := c.encodeBlockCapnp(op, seq, hash[:], lba, timestamp, data, size)
		if err != nil {
			return block, err
		}
		return block, c.bw.Flush()
	}

	okToSend := true

	for i := 0; i < sendRetryNum+1; i++ {
		if okToSend {
			block, err = send()
			if err == nil {
				return
			}

			if _, ok := err.(net.Error); !ok {
				return // no need to rety if it is not network error.
			}
		}

		// First sleep = 0 second,
		// so we don't need to sleep in case of simple closed connection.
		// We sleep in next iteration because there might be something error in
		// the network connection or the tlog server that need time to be recovered.
		time.Sleep(time.Duration(i) * sendSleepTime)

		if err = c.connect(c.blockBuffer.MinSequence()); err != nil {
			log.Infof("tlog client : reconnect attemp(%v) failed:%v", i, err)
			okToSend = false
		} else {
			okToSend = true
		}
	}
	return
}

// Close the open connection, making this client invalid
func (c *Client) Close() error {
	c.cancelFunc()
	return c.conn.Close()
}
