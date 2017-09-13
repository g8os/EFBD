package tlogclient

import (
	"bufio"
	"context"
	"errors"
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

var (
	errMaxSendRetry = errors.New("max send retry reached")

	// ErrClientClosed returned when client do something when
	// it already closed
	ErrClientClosed = errors.New("client already closed")

	// ErrWaitSlaveSyncTimeout returned when nbd slave sync couldn't be finished
	ErrWaitSlaveSyncTimeout = errors.New("wait nbd slave sync timed out")

	// ErrFlushFailed returned when client failed to do flush
	ErrFlushFailed = errors.New("tlogserver failed to flush")
)

// Response defines a response from tlog server
// TODO: find a better way to return a response to the client's user,
//       as it might not make sense for all commands to return sequences.
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
	servers         []string
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
	stopped       bool

	waitSlaveSyncCond *sync.Cond

	serverAddrLock sync.Mutex
}

// New creates a new tlog client for a vdisk with 'addrs' is the tlogserver addresses.
// Client is going to use first address and then move to next addresses if the first address
// is failed.
// 'firstSequence' is the first sequence number this client is going to send.
// Set 'resetFirstSeq' to true to force reset the vdisk first/expected sequence.
// The client is not goroutine safe.
func New(servers []string, vdiskID string, firstSequence uint64, resetFirstSeq bool) (*Client, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := &Client{
		servers:           servers,
		vdiskID:           vdiskID,
		blockBuffer:       blockbuffer.NewBuffer(resendTimeoutDur),
		ctx:               ctx,
		cancelFunc:        cancelFunc,
		waitSlaveSyncCond: sync.NewCond(&sync.Mutex{}),
	}

	if err := client.connect(firstSequence, resetFirstSeq); err != nil {
		return nil, err
	}

	go client.resender()
	return client, nil
}

// ChangeServerAddresses change servers to the given servers
func (c *Client) ChangeServerAddresses(servers []string) {
	c.serverAddrLock.Lock()
	defer c.serverAddrLock.Unlock()

	if len(servers) == 0 {
		return
	}
	log.Infof("tlogclient vdisk '%v' change server addrs to '%v'", c.vdiskID, servers)

	// reconnect client if current server not exist in the
	// new addresses
	curExist := func() bool {
		for _, server := range servers {
			if c.servers[0] == server {
				return true
			}
		}
		return false
	}()
	c.servers = servers

	if !curExist {
		log.Infof("tlogclient vdisk '%v' current server is not in new address, close it", c.vdiskID)
		c.conn.Close()
	}

}

// shift server address move current active client to the back
// and use 2nd entry as main server
func (c *Client) shiftServer() {
	c.serverAddrLock.Lock()
	defer c.serverAddrLock.Unlock()

	c.servers = append(c.servers[1:], c.servers[0])
}

func (c *Client) curServerAddress() string {
	c.serverAddrLock.Lock()
	defer c.serverAddrLock.Unlock()

	return c.servers[0]
}

// reconnect to server
// it must be called under wLock
func (c *Client) reconnect(closedTime time.Time) error {
	var err error

	if c.stopped {
		return ErrClientClosed
	}

	if c.lastConnected.After(closedTime) { // another goroutine made the connection work again
		return nil
	}

	log.Info("tlogclient reconnect")

	for {
		// try other server
		c.shiftServer()

		if err = c.connect(c.blockBuffer.LastFlushed()+1, false); err == nil {
			c.lastConnected = time.Now()

			// if reconnect success, sent all unflushed blocks
			// because we don't know what happens in servers.
			// it might crashed
			c.blockBuffer.SetResendAll()
			return nil
		}

	}
}

// connect to server
func (c *Client) connect(firstSequence uint64, resetFirstSeq bool) (err error) {
	if c.conn != nil {
		c.conn.CloseRead() // interrupt the receiver
	}

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
			seq := block.Sequence()

			// check it once again, make sure this block still
			// need to be re-send
			if !c.blockBuffer.NeedResend(seq) {
				continue
			}

			data, err := block.Data()
			if err != nil {
				log.Errorf("client resender failed to get data block:%v", err)
				continue
			}

			err = c.Send(block.Operation(), seq, block.Index(), block.Timestamp(), data)
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
					c.reconnectFromRead(err)
					continue
				}

				if tr != nil {
					switch tr.Status {
					case tlog.BlockStatusRecvOK:
						if len(tr.Sequences) > 0 { // should always be true, but we anticipate.
							c.blockBuffer.SetSent(tr.Sequences[0])
						}
					case tlog.BlockStatusFlushOK:
						c.blockBuffer.SetFlushed(tr.Sequences)
					case tlog.BlockStatusWaitNbdSlaveSyncReceived:
						log.Info("tlog client receive BlockStatusWaitNbdSlaveSyncReceived")
						c.signalCond(c.waitSlaveSyncCond)
					case tlog.BlockStatusFlushFailed:
						if err := c.reconnectFromRead(ErrFlushFailed); err != nil {
							reChan <- &Result{
								Err: ErrFlushFailed,
							}
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

// reconnect from read do re-connect from reading goroutine
func (c *Client) reconnectFromRead(errCause error) error {
	closedTime := time.Now()
	log.Infof("tlogclient : reconnect from read because of : %v", errCause)

	c.wLock.Lock()
	defer c.wLock.Unlock()

	if err := c.reconnect(closedTime); err != nil {
		log.Infof("tlogclient: reconnect because `%v` failed: %v", errCause, err)
		return err
	}
	return nil
}

// recvOne receive one response
func (c *Client) recvOne() (*Response, error) {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	// decode capnp and build response
	tr, err := c.decodeTlogResponse(c.rd)
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
	genericConn, err := net.DialTimeout("tcp", c.curServerAddress(), time.Second)
	if err != nil {
		return err
	}

	conn := genericConn.(*net.TCPConn)

	conn.SetKeepAlive(true)
	c.conn = conn
	c.bw = bufio.NewWriter(conn)
	c.rd = conn
	return nil
}

// ForceFlushAtSeq force flush at given sequence
// NOTE : this func doesn't have retry logic, user need to
// add it
func (c *Client) ForceFlushAtSeq(seq uint64) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	sender := func() (interface{}, error) {
		if err := c.encodeForceFlushAtSeq(c.bw, seq); err != nil {
			return nil, err
		}
		return nil, c.bw.Flush()
	}

	_, err := c.sendReconnect(sender)
	return err
}

// WaitNbdSlaveSync commands tlog server to wait
// for nbd slave to be fully synced
func (c *Client) WaitNbdSlaveSync() error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	sender := func() (interface{}, error) {
		if err := c.encodeWaitNBDSlaveSync(c.bw); err != nil {
			return nil, err
		}
		return nil, c.bw.Flush()
	}

	// start the waiter now, so we don't miss the reply
	doneCh := c.waitCond(c.waitSlaveSyncCond)

	if _, err := c.sendReconnect(sender); err != nil {
		return err
	}

	// the actual wait
	for {
		select {
		case <-time.After(20 * time.Second):
			c.signalCond(c.waitSlaveSyncCond)
			return ErrWaitSlaveSyncTimeout
		case <-doneCh:
			return nil
		}
	}
}

// wait for a condition to happens
// it happens when the channel is closed
func (c *Client) waitCond(cond *sync.Cond) chan struct{} {
	doneCh := make(chan struct{})        // channel to wait for waiter completion
	waiterStarted := make(chan struct{}) // channel to wait for waiter start

	go func() {
		cond.L.Lock()
		waiterStarted <- struct{}{}
		cond.Wait()
		cond.L.Unlock()
		close(doneCh)
	}()

	<-waiterStarted
	return doneCh
}

// Signal to a condition variable
func (c *Client) signalCond(cond *sync.Cond) {
	cond.L.Lock()
	cond.Signal()
	cond.L.Unlock()
}

// Send sends the transaction tlog to server.
// It returns error in these cases:
// - failed to encode the capnp.
// - failed to recover from broken network connection.
func (c *Client) Send(op uint8, seq uint64, index int64, timestamp int64, data []byte) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	block, err := c.send(op, seq, index, timestamp, data)
	if err == nil && block != nil {
		c.blockBuffer.Add(block)
	}
	return err
}

// send tlog block to server
func (c *Client) send(op uint8, seq uint64, index int64, timestamp int64, data []byte) (*schema.TlogBlock, error) {
	hash := zerodisk.HashBytes(data)

	sender := func() (interface{}, error) {
		block, err := c.encodeBlockCapnp(c.bw, op, seq, index, hash[:], timestamp, data)
		if err != nil {
			return block, err
		}

		return block, c.bw.Flush()
	}

	// send it
	blockIf, err := c.sendReconnect(sender)
	if err != nil {
		return nil, err
	}

	// convert the return value
	block, ok := blockIf.(*schema.TlogBlock)
	if !ok {
		return nil, fmt.Errorf("can't convert from interface{} to block")
	}

	return block, nil
}

// generic funct that execute a sender and do connection reconnect if needed
func (c *Client) sendReconnect(sender func() (interface{}, error)) (interface{}, error) {
	var err error
	var ret interface{}

	for i := 0; i < sendRetryNum+1; i++ {
		if err != nil {
			if _, ok := err.(net.Error); !ok {
				return nil, err // no need to rety if it is not network error.
			}

			closedTime := time.Now()

			// First sleep = 0 second,
			// so we don't need to sleep in case of simple closed connection.
			// We sleep in next iteration because there might be something error in
			// the network connection or the tlog server that need time to be recovered.
			time.Sleep(time.Duration(i-1) * sendSleepTime)

			if err = c.reconnect(closedTime); err != nil {
				log.Infof("tlog client : reconnect from send attemp(%v) failed:%v", i, err)
				continue
			}
		}

		ret, err = sender()
		if err == nil {
			return ret, nil
		}
		log.Errorf("tlogclient failed to send: %v", err)

	}
	return nil, errMaxSendRetry
}

// Close the open connection, making this client invalid.
// It is user responsibility to call this function.
func (c *Client) Close() error {
	c.cancelFunc()

	c.wLock.Lock()
	defer c.wLock.Unlock()

	c.stopped = true

	if c.conn != nil {
		c.conn.CloseRead() // interrupt the receiver
		return c.conn.Close()
	}
	return nil
}
