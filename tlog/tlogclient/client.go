package tlogclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/blockbuffer"
)

const (
	sendRetryNum     = 5
	sendSleepTime    = 500 * time.Millisecond // sleep duration before retrying the Send
	resendTimeoutDur = 2 * time.Second        // duration to wait before re-send the tlog.
	readTimeout      = 2 * time.Second
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

	lastConnected time.Time
}

// New creates a new tlog client for a vdisk with 'addr' is the tlogserver address.
// 'firstSequence' is the first sequence number this client is going to send.
// Set 'resetFirstSeq' to true to force reset the vdisk first/expected sequence.
// The client is not goroutine safe.
func New(addr, vdiskID string, firstSequence uint64, resetFirstSeq bool) (*Client, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := &Client{
		addr:        addr,
		vdiskID:     vdiskID,
		blockBuffer: blockbuffer.NewBuffer(resendTimeoutDur),
		ctx:         ctx,
		cancelFunc:  cancelFunc,
	}

	if err := client.connect(firstSequence, resetFirstSeq); err != nil {
		return nil, err
	}

	go client.resender()
	return client, nil
}

// reconnect to server
// it must be called under wLock
func (c *Client) reconnect(closedTime time.Time) (err error) {
	if c.lastConnected.After(closedTime) { // another goroutine made the connection work again
		return
	}

	if err = c.connect(c.blockBuffer.MinSequence(), false); err == nil {
		c.lastConnected = time.Now()
	}
	return
}

// connect to server
func (c *Client) connect(firstSequence uint64, resetFirstSeq bool) (err error) {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	defer func() {
		if err == nil {
			c.lastConnected = time.Now()
		}
	}()

	if err = c.createConn(); err != nil {
		return fmt.Errorf("client couldn't be created: %s", err.Error())
	}

	if err = c.handshake(firstSequence, resetFirstSeq); err != nil {
		if errClose := c.conn.Close(); errClose != nil {
			log.Debug("couldn't close open connection of invalid client:", errClose)
		}
		return fmt.Errorf("client handshake failed: %s", err.Error())
	}
	return nil
}

// goroutine which re-send the block.
func (c *Client) resender() {
	timeoutCh := c.blockBuffer.TimedOut(c.ctx)
	for {
		select {
		case <-c.ctx.Done():
			return
		case block := <-timeoutCh:
			data, err := block.Data()
			if err != nil {
				log.Errorf("client resender failed to get data block:%v", err)
				continue
			}

			err = c.Send(block.Operation(), block.Sequence(), block.Offset(), block.Timestamp(), data, block.Size())
			if err != nil {
				log.Errorf("client resender failed to send data:%v", err)
			}
		}
	}
}

func (c *Client) handshake(firstSequence uint64, resetFirstSeq bool) error {
	// send handshake request
	err := c.encodeHandshakeCapnp(firstSequence, resetFirstSeq)
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
			select {
			case <-c.ctx.Done():
				return
			default:
				tr, err := c.recvOne()

				nerr, isNetErr := err.(net.Error)

				// it is timeout error
				// client can't really read something
				if isNetErr && nerr.Timeout() {
					continue
				}

				// EOF and other network error triggers reconnection
				if isNetErr || err == io.EOF {
					log.Infof("tlogclient : reconnect from read because of : %v", err)
					err := func() error {
						c.wLock.Lock()
						defer c.wLock.Unlock()
						return c.reconnect(time.Now())
					}()
					if err != nil {
						log.Infof("Recv failed to reconnect: %v", err)
					}
					continue
				}

				if tr != nil && len(tr.Sequences) > 0 {
					// if it successfully received by server, delete from buffer
					if tr.Status == tlog.BlockStatusRecvOK {
						if len(tr.Sequences) > 0 { // should always be true, but we anticipate.
							c.blockBuffer.Delete(tr.Sequences[0])
						}
					}
				}

				reChan <- &Result{
					Resp: tr,
					Err:  err,
				}
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
	tr, err := c.decodeBlockResponse(c.rd)
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

// ForceFlushAtSeq force flush at given sequence
func (c *Client) ForceFlushAtSeq(seq uint64) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	// TODO :
	// - add reconnect
	// - add resend
	return c.forceFlushAtSeq(seq)
}

func (c *Client) forceFlushAtSeq(seq uint64) error {
	if err := tlog.WriteMessageType(c.bw, tlog.MessageForceFlushAtSeq); err != nil {
		return err
	}
	if err := c.encodeSendCommand(c.bw, tlog.MessageForceFlushAtSeq, seq); err != nil {
		return err
	}
	return c.bw.Flush()
}

// Send sends the transaction tlog to server.
// It returns error in these cases:
// - failed to encode the capnp.
// - failed to recover from broken network connection.
func (c *Client) Send(op uint8, seq, offset, timestamp uint64,
	data []byte, size uint64) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	block, err := c.send(op, seq, offset, timestamp, data, size)
	if err == nil && block != nil {
		c.blockBuffer.Add(block)
	}
	return err
}

// send tlog block to server
func (c *Client) send(op uint8, seq, offset, timestamp uint64,
	data []byte, size uint64) (*schema.TlogBlock, error) {
	hash := zerodisk.HashBytes(data)

	send := func() (*schema.TlogBlock, error) {
		if err := tlog.WriteMessageType(c.bw, tlog.MessageTlogBlock); err != nil {
			return nil, err
		}

		block, err := c.encodeBlockCapnp(c.bw, op, seq, hash[:], offset, timestamp, data, size)
		if err != nil {
			return block, err
		}

		return block, c.bw.Flush()
	}

	var err error
	var block *schema.TlogBlock

	okToSend := true

	for i := 0; i < sendRetryNum+1; i++ {
		if okToSend {
			block, err = send()
			if err == nil {
				return block, nil
			}

			if _, ok := err.(net.Error); !ok {
				return nil, err // no need to rety if it is not network error.
			}
		}
		closedTime := time.Now()

		// First sleep = 0 second,
		// so we don't need to sleep in case of simple closed connection.
		// We sleep in next iteration because there might be something error in
		// the network connection or the tlog server that need time to be recovered.
		time.Sleep(time.Duration(i) * sendSleepTime)

		if err = c.reconnect(closedTime); err != nil {
			log.Infof("tlog client : reconnect attemp(%v) failed:%v", i, err)
			okToSend = false
		} else {
			okToSend = true
		}
	}
	return nil, err
}

// Close the open connection, making this client invalid.
// It is user responsibility to call this function.
func (c *Client) Close() error {
	c.cancelFunc()

	c.wLock.Lock()
	defer c.wLock.Unlock()

	c.rLock.Lock()
	c.rLock.Unlock()

	return c.conn.Close()
}
