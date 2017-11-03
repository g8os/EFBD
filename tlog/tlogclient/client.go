package tlogclient

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/tlogclient/blockbuffer"
)

const (
	readTimeout          = 2 * time.Second
	resendTimeoutDur     = 2 * time.Second // duration to wait before re-send the tlog.
	failedFlushSleepTime = 10 * time.Second
)

var (
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

	commandCh      chan command
	retryCommandCh chan command
	respCh         chan *Result

	ctx        context.Context
	cancelFunc context.CancelFunc

	waitSlaveSyncCond *sync.Cond
	disconnectedCond  *sync.Cond

	mux                  sync.Mutex
	stopped              bool
	serverReady          bool
	serverReadyCh        chan struct{}
	curServerFailedFlush bool
}

// New creates a new tlog client for a vdisk with 'addrs' is the tlogserver addresses.
// Client is going to use first address and then move to next addresses if the first address
// is failed.
// The client is not goroutine safe.
func New(servers []string, vdiskID string) (client *Client, err error) {
	client, err = newClient(servers, vdiskID)
	if err != nil {
		return
	}
	go client.run(client.ctx)
	return
}
func newClient(servers []string, vdiskID string) (*Client, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	client := &Client{
		servers:           servers,
		vdiskID:           vdiskID,
		blockBuffer:       blockbuffer.NewBuffer(resendTimeoutDur),
		ctx:               ctx,
		cancelFunc:        cancelFunc,
		waitSlaveSyncCond: sync.NewCond(&sync.Mutex{}),
		disconnectedCond:  sync.NewCond(&sync.Mutex{}),

		// channel for command
		commandCh: make(chan command),

		// channel for retried command
		retryCommandCh: make(chan command, 2),

		// three is big enough size:
		// - one waiting to be fetched by application/user
		// - one for the response we just received
		// - one for (hopefully) optimization, so we can read from network
		//	 before needed and then put it to this channel.
		respCh: make(chan *Result, 3),

		serverReadyCh: make(chan struct{}, 1),
	}

	return client, client.connect()
}

func (c *Client) run(ctx context.Context) {
	go c.resender()
	for !c.isStopped() {
		curCtx, cancelFunc := context.WithCancel(ctx)
		// run receiver and sender
		sendDoneCh := c.runSender(curCtx, cancelFunc)
		recvDoneCh := c.runReceiver(curCtx, cancelFunc)

		// wait for receiver and sender to be finished
		<-sendDoneCh
		<-recvDoneCh

		// reconnect
		reconnected := false
		for !reconnected && !c.isStopped() {
			select {
			case <-ctx.Done():
				return
			default:
				err := c.reconnect(time.Now())
				if err != nil {
					log.Errorf("reconnect failed : %v", err)
				} else {
					reconnected = true
				}
			}
		}
	}
}

func (c *Client) sendCmd(cmd command) error {
	block, err := cmd.encodeSend(c.bw)
	if err != nil {
		return err
	}
	if err := c.bw.Flush(); err != nil {
		return err
	}

	if block != nil {
		c.blockBuffer.Add(block)
	}
	return nil
}
func (c *Client) runSender(ctx context.Context, cancelFunc context.CancelFunc) <-chan struct{} {
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			c.conn.CloseRead()
			cancelFunc()
			doneCh <- struct{}{}
		}()

		// retry command from previous run loop
		numCmdRetry := len(c.retryCommandCh)
		for i := 0; i < numCmdRetry; i++ {
			cmd := <-c.retryCommandCh
			if err := c.sendCmd(cmd); err != nil {
				log.Errorf("Failed when retry command = %v", err)
				c.retryCommandCh <- cmd
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case cmd := <-c.commandCh:
				if err := c.sendCmd(cmd); err != nil {
					log.Infof("Failed to send command = %v", err)
					c.retryCommandCh <- cmd
					return
				}

			}
		}
	}()
	return doneCh
}

func (c *Client) runReceiver(ctx context.Context, cancelFunc context.CancelFunc) <-chan struct{} {
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			c.conn.CloseWrite()
			cancelFunc()
			doneCh <- struct{}{}
		}()

		for {
			select {
			case <-ctx.Done():
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
				if isNetErr || errors.Cause(err) == io.EOF {
					if !c.isStopped() {
						log.Errorf("error while reading: %v", err)
					}
					return
				}

				if tr == nil {
					c.respCh <- &Result{
						Err: err,
					}
					continue
				}

				switch tr.Status {
				case tlog.BlockStatusRecvOK:
					if len(tr.Sequences) > 0 { // should always be true, but we anticipate.
						c.blockBuffer.SetSent(tr.Sequences[0])
					}
				case tlog.BlockStatusReady:
					if c.Ready() {
						continue
					}

					seq := tr.Sequences[0]
					if seq != 0 {
						c.blockBuffer.SetLastFlushed(seq)
					}

					c.mux.Lock()
					c.serverReady = true
					c.mux.Unlock()
					c.serverReadyCh <- struct{}{}
					log.Infof("tlogserver is ready")
				case tlog.BlockStatusFlushOK:
					c.blockBuffer.SetFlushed(tr.Sequences)
				case tlog.BlockStatusWaitNbdSlaveSyncReceived:
					log.Info("tlog client receive BlockStatusWaitNbdSlaveSyncReceived")
					c.signalCond(c.waitSlaveSyncCond)
				case tlog.BlockStatusFlushFailed:
					log.Errorf("tlogclient receive BlockStatusFlushFailed for vdisk: %v", c.vdiskID)
					c.setCurServerFailedFlushStatus(true)
				case tlog.BlockStatusDisconnected:
					c.signalCond(c.disconnectedCond)
				}

				c.respCh <- &Result{
					Resp: tr,
					Err:  err,
				}
			}
		}
	}()
	return doneCh
}

// ChangeServerAddresses change servers to the given servers
func (c *Client) ChangeServerAddresses(servers []string) {
	c.mux.Lock()
	defer c.mux.Unlock()

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
	c.mux.Lock()
	defer c.mux.Unlock()

	c.servers = append(c.servers[1:], c.servers[0])
}

func (c *Client) curServerAddress() string {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.servers[0]
}

func (c *Client) setCurServerFailedFlushStatus(status bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.curServerFailedFlush = status
}

func (c *Client) getCurServerFailedFlushStatus() bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.curServerFailedFlush
}

// reconnect to server
func (c *Client) reconnect(closedTime time.Time) error {
	var err error

	log.Info("tlogclient reconnect")

	for {
		// try other server
		c.shiftServer()

		if err = c.connect(); err == nil {
			// if reconnect success, sent all unflushed blocks
			// because we don't know what happens in servers.
			// it might crashed
			c.blockBuffer.SetResendAll()
			return nil
		}

	}
}

// connect to server
func (c *Client) connect() error {
	if c.conn != nil {
		c.conn.CloseRead() // interrupt the receiver
	}

	if c.getCurServerFailedFlushStatus() {
		// sleep for a while
		// in case of failed flush
		// to give the time for server to recover
		time.Sleep(failedFlushSleepTime)
	}

	c.setCurServerFailedFlushStatus(false)

	if err := c.createConn(); err != nil {
		return errors.Wrap(err, "client couldn't be created")
	}

	ready, err := c.handshake()
	if err != nil {
		if errClose := c.conn.Close(); errClose != nil {
			log.Debug("couldn't close open connection of invalid client:", errClose)
		}
		return errors.Wrap(err, "client handshake failed")
	}

	c.mux.Lock()
	c.serverReady = ready
	c.mux.Unlock()

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

// do handshaking process to tlogserver
// it returns true if tlogserver is ready
func (c *Client) handshake() (ready bool, err error) {
	// send handshake request
	err = c.encodeHandshakeCapnp()
	if err != nil {
		return ready, err
	}
	err = c.bw.Flush()
	if err != nil {
		return ready, err
	}

	// receive handshake response
	resp, err := c.decodeHandshakeResponse()
	if err != nil {
		return ready, err
	}

	// check server response status
	err = tlog.HandshakeStatus(resp.Status()).Error()
	if err != nil {
		return ready, err
	}

	serverLastFlushed := resp.LastFlushedSequence()
	if serverLastFlushed != 0 {
		c.blockBuffer.SetLastFlushed(serverLastFlushed)
	}

	ready = !resp.WaitTlogReady()

	// all checks out, ready to go!
	return ready, nil
}

// Recv get channel of responses and errors (Result)
func (c *Client) Recv() <-chan *Result {
	return c.respCh
}

// recvOne receive one response
func (c *Client) recvOne() (*Response, error) {
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
func (c *Client) ForceFlushAtSeq(seq uint64) error {
	c.commandCh <- cmdForceFlushAtSeq{seq: seq}
	return nil
}

// WaitNbdSlaveSync commands tlog server to wait
// for nbd slave to be fully synced
func (c *Client) WaitNbdSlaveSync() error {
	// start the waiter now, so we don't miss the reply
	doneCh := c.waitCond(c.waitSlaveSyncCond)

	// send it
	c.commandCh <- cmdWaitNbdSlaveSync{}

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
	cmd := cmdBlock{
		op:         op,
		seq:        seq,
		index:      index,
		timestamp:  timestamp,
		data:       data,
		segmentBuf: c.capnpSegmentBuf,
	}
	c.commandCh <- cmd
	return nil
}

// LastFlushedSequence returns tlog last flushed sequence
func (c *Client) LastFlushedSequence() uint64 {
	return c.blockBuffer.LastFlushed()
}

// Close the open connection, making this client invalid.
// It is user responsibility to call this function.
func (c *Client) Close() error {
	c.mux.Lock()
	c.stopped = true
	c.mux.Unlock()
	c.cancelFunc()

	if c.conn != nil {
		c.conn.CloseRead() // interrupt the receiver
		return c.conn.Close()
	}
	return nil
}

// Ready returns true if server is ready and thus this client
// is ready to be used
func (c *Client) Ready() bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.serverReady
}

func (c *Client) WaitReady() {
	if c.Ready() {
		return
	}
	<-c.serverReadyCh
	c.mux.Lock()
	c.serverReady = true
	c.mux.Unlock()
}

// Disconnect disconnects client gracefully
// It is on progress func which can only be used in unit test
// See https://github.com/zero-os/0-Disk/issues/426 for the progress
func (c *Client) Disconnect() error {
	c.mux.Lock()
	c.stopped = true
	c.mux.Unlock()

	doneCh := c.waitCond(c.disconnectedCond)

	c.commandCh <- cmdDisconnect{}

	select {
	case <-time.After(10 * time.Second):
		c.signalCond(c.disconnectedCond)
		return errors.New("disconnect timed out")
	case <-doneCh:
		return nil
	}
}

func (c *Client) isStopped() bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.stopped
}
