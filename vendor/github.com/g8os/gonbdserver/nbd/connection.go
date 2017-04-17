package nbd

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/glendc/go-mini-log"

	"golang.org/x/net/context"
)

// Map of configuration text to TLS versions
var tlsVersionMap = map[string]uint16{
	"ssl3.0": tls.VersionSSL30,
	"tls1.0": tls.VersionTLS10,
	"tls1.1": tls.VersionTLS11,
	"tls1.2": tls.VersionTLS12,
}

// Map of configuration text to TLS authentication strategies
var tlsClientAuthMap = map[string]tls.ClientAuthType{
	"none":          tls.NoClientCert,
	"request":       tls.RequestClientCert,
	"require":       tls.RequireAnyClientCert,
	"verify":        tls.VerifyClientCertIfGiven,
	"requireverify": tls.RequireAndVerifyClientCert,
}

// ConnectionParameters holds parameters for each inbound connection
type ConnectionParameters struct {
	ConnectionTimeout time.Duration // maximum time to complete negotiation
}

// Connection holds the details for each connection
type Connection struct {
	params             *ConnectionParameters // parameters
	conn               net.Conn              // the connection that is used as the NBD transport
	plainConn          net.Conn              // the unencrypted (original) connection
	tlsConn            net.Conn              // the TLS encrypted connection
	logger             log.Logger            // a logger
	listener           *Listener             // the listener than invoked us
	export             *Export               // a pointer to the export
	backend            Backend               // the backend implementation
	wg                 sync.WaitGroup        // a waitgroup for the session; we mark this as done on exit
	repCh              chan []byte           // a channel of replies that have to be sent
	numInflight        int64                 // number of inflight requests
	name               string                // the name of the connection for logging purposes
	disconnectReceived int64                 // more then 0 if disconnect has been received

	killCh    chan struct{} // closed by workers to indicate a hard close is required
	killed    bool          // true if killCh closed already
	killMutex sync.Mutex    // protects killed
}

// Backend is an interface implemented by the various backend drivers
type Backend interface {
	WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int64, error)     // write data to w at offset, with force unit access optional
	WriteZeroesAt(ctx context.Context, offset, length int64, fua bool) (int64, error) // write zeroes to w at offset, with force unit access optional
	ReadAt(ctx context.Context, offset, length int64) ([]byte, error)                 // read from o b at offset
	TrimAt(ctx context.Context, offset, length int64) (int64, error)                  // trim
	Flush(ctx context.Context) error                                                  // flush
	Close(ctx context.Context) error                                                  // close
	Geometry(ctx context.Context) (Geometry, error)                                   // size, minimum BS, preferred BS, maximum BS
	HasFua(ctx context.Context) bool                                                  // does the driver support FUA?
	HasFlush(ctx context.Context) bool                                                // does the driver support flush?
}

// BackendGenerator is a generator function type that generates a backend
type BackendGenerator func(ctx context.Context, e *ExportConfig) (Backend, error)

// backendMap is a map between backends and the generator function for them
var backendMap = make(map[string]BackendGenerator)

// Export contains the details of an export
type Export struct {
	size               uint64 // size in bytes
	minimumBlockSize   uint64 // minimum block size
	preferredBlockSize uint64 // preferred block size
	maximumBlockSize   uint64 // maximum block size
	memoryBlockSize    uint64 // block size for memory chunks
	exportFlags        uint16 // export flags in NBD format
	name               string // name of the export
	description        string // description of the export
	readonly           bool   // true if read only
	tlsonly            bool   // true if only to be served over tls
}

// Geometry information for a backend
type Geometry struct {
	Size               uint64
	MinimumBlockSize   uint64
	PreferredBlockSize uint64
	MaximumBlockSize   uint64
}

// Reply is an internal structure for propagating replies
// onto the reply goroutine to be sent from there
type Reply struct {
	nbdRep  nbdReply // the reply in nbd format
	payload []byte
}

// NewConnection returns a new Connection object
func NewConnection(listener *Listener, logger log.Logger, conn net.Conn) (*Connection, error) {
	if logger == nil {
		return nil, errors.New("NewConnection requires a non-nil logger")
	}

	params := &ConnectionParameters{
		ConnectionTimeout: time.Second * 5,
	}
	c := &Connection{
		plainConn: conn,
		listener:  listener,
		logger:    logger,
		params:    params,
	}
	return c, nil
}

// errorCodeFromGolangError translates an error returned by golang
// into an NBD error code used for replies
//
// This function could do with some serious work!
func errorCodeFromGolangError(error) uint32 {
	//  TODO: relate the return value to the given error
	return NBD_EIO
}

// isClosedErr returns true if the error related to use of a closed connection.
//
// this is particularly foul but is used to surpress errors that relate to use of a closed connection. This is because
// they only arise as we ourselves close the connection to get blocking reads/writes to safely terminate, and thus do
// not want to report them to the user as an error
func isClosedErr(err error) bool {
	return strings.HasSuffix(err.Error(), "use of closed network connection") // YUCK!
}

// turn a nbdReply into a payload ready to send
func (c *Connection) nbdReplyToBytes(rep *nbdReply) (payload []byte, err error) {
	var buffer bytes.Buffer
	err = binary.Write(&buffer, binary.BigEndian, rep)
	if err == nil {
		payload = buffer.Bytes()
	}

	return
}

// sendHeader and returns true in case the sending was OK
func (c *Connection) sendHeader(ctx context.Context, rep *nbdReply) bool {
	payload, err := c.nbdReplyToBytes(rep)
	if err != nil {
		c.logger.Infof("Client %s couldn't send reply", c.name)
		return false
	}

	return c.sendPayload(ctx, payload)
}

// sendPayload and returns true in case the sending was OK
func (c *Connection) sendPayload(ctx context.Context, payload []byte) bool {
	atomic.AddInt64(&c.numInflight, 1) // one more in flight
	select {
	case c.repCh <- payload:
	case <-ctx.Done():
		return false
	}

	return true
}

// reply handles the sending of replies over the connection
// done async over a goroutine
func (c *Connection) reply(ctx context.Context) {
	defer func() {
		c.logger.Infof("Replyer exiting for %s", c.name)
		c.kill(ctx)
		c.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-c.repCh:
			if !ok {
				return
			}

			n, err := c.conn.Write(payload)
			if err != nil {
				c.logger.Infof(
					"Client %s cannot write reply: %s", c.name, err)
				return
			}
			if en := len(payload); en != n {
				c.logger.Infof(
					"Client %s cannot write reply: written %d instead of %d bytes",
					c.name, n, en)
				return
			}

			atomic.AddInt64(&c.numInflight, -1) // one less in flight
		}
	}
}

// receive requests, process them and
// dispatch the replies to be sent over another goroutine
func (c *Connection) receive(ctx context.Context) {
	defer func() {
		c.logger.Infof("Receiver exiting for %s", c.name)
		c.kill(ctx)
		c.wg.Done()
	}()

	for {
		// get request
		var req nbdRequest
		if err := binary.Read(c.conn, binary.BigEndian, &req); err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					c.logger.Infof("Client %s timeout, closing connection", c.name)
					return
				}
			}
			if isClosedErr(err) {
				// Don't report this - we closed it
				return
			}
			if err == io.EOF {
				c.logger.Infof("Client %s closed connection abruptly", c.name)
			} else {
				c.logger.Infof("Client %s could not read request: %s", c.name, err)
			}
			return
		}

		if req.NbdRequestMagic != NBD_REQUEST_MAGIC {
			c.logger.Infof("Client %s had bad magic number in request", c.name)
			return
		}

		// handle req flags
		flags, ok := CmdTypeMap[int(req.NbdCommandType)]
		if !ok {
			c.logger.Infof(
				"Client %s unknown command %d",
				c.name, req.NbdCommandType)
			return
		}

		if flags&CMDT_SET_DISCONNECT_RECEIVED != 0 {
			// we process this here as commands may otherwise be processed out
			// of order and per the spec we should not receive any more
			// commands after receiving a disconnect
			atomic.StoreInt64(&c.disconnectReceived, 1)
		}

		if flags&CMDT_CHECK_LENGTH_OFFSET != 0 {
			length := uint64(req.NbdLength)
			if length <= 0 || length+req.NbdOffset > c.export.size {
				c.logger.Infof("Client %s gave bad offset or length", c.name)
				return
			}

			if length&(c.export.minimumBlockSize-1) != 0 || req.NbdOffset&(c.export.minimumBlockSize-1) != 0 || length > c.export.maximumBlockSize {
				c.logger.Infof("Client %s gave offset or length outside blocksize paramaters cmd=%d (len=%08x,off=%08x,minbs=%08x,maxbs=%08x)", c.name, req.NbdCommandType, req.NbdLength, req.NbdOffset, c.export.minimumBlockSize, c.export.maximumBlockSize)
				return
			}
		}

		if flags&CMDT_CHECK_NOT_READ_ONLY != 0 && c.export.readonly {
			payload, err := c.nbdReplyToBytes(&nbdReply{
				NbdReplyMagic: NBD_REPLY_MAGIC,
				NbdHandle:     req.NbdHandle,
				NbdError:      NBD_EPERM,
			})
			if err != nil {
				c.logger.Infof("Client %s couldn't send error (NBD_EPERM) reply", c.name)
				return
			}

			atomic.AddInt64(&c.numInflight, 1) // one more in flight
			select {
			case c.repCh <- payload:
			case <-ctx.Done():
				return
			}

			return
		}

		fua := req.NbdCommandFlags&NBD_CMD_FLAG_FUA != 0

		length := uint64(req.NbdLength) // make length local
		offset := req.NbdOffset         // make offset local

		memoryBlockSize := c.export.memoryBlockSize
		blocklen := memoryBlockSize
		if blocklen > length {
			blocklen = length
		}

		//Make sure the reads are until the blockboundary
		offsetInsideBlock := offset % memoryBlockSize
		if blocklen+offsetInsideBlock > memoryBlockSize {
			blocklen = memoryBlockSize - offsetInsideBlock
		}

		nbdRep := &nbdReply{
			NbdReplyMagic: NBD_REPLY_MAGIC,
			NbdHandle:     req.NbdHandle,
			NbdError:      0,
		}

		// handle request command
		switch req.NbdCommandType {
		case NBD_CMD_READ:
			// be positive, and send header already!
			if !c.sendHeader(ctx, nbdRep) {
				return // ouch
			}

			var i uint64
			totalLength := offsetInsideBlock + length
			readParts := totalLength / memoryBlockSize
			if totalLength%memoryBlockSize != 0 {
				readParts++ // 1 extra because of block alignment
			}

			// create channels for reading concurrently,
			// while still replying in order
			readChannels := make([]chan []byte, readParts)
			for i = 0; i < readParts; i++ {
				readChannels[i] = make(chan []byte, 1)
				go func(out chan []byte, offset int64, blocklen int64) {
					payload, err := c.backend.ReadAt(ctx, offset, blocklen)
					if err != nil {
						c.logger.Infof("Client %s got read I/O error: %s", c.name, err)
						out <- nil
						return
					} else if actualLength := int64(len(payload)); actualLength != blocklen {
						c.logger.Infof("Client %s got incomplete read (%d != %d) at offset %d", c.name, actualLength, blocklen, offset)
						out <- nil
						return
					}

					out <- payload
				}(readChannels[i], int64(offset), int64(blocklen))

				length -= blocklen
				offset += blocklen

				blocklen = memoryBlockSize
				if blocklen > length {
					blocklen = length
				}
			}

			var payload []byte
			for i = 0; i < readParts; i++ {
				payload = <-readChannels[i]
				if payload == nil {
					return // an error occured
				}

				if !c.sendPayload(ctx, payload) {
					return // an error occured
				}
			}

		case NBD_CMD_WRITE:
			var cn int
			var err error
			wg := sync.WaitGroup{}
			for blocklen > 0 {
				wBuffer := make([]byte, blocklen)
				cn, err = io.ReadFull(c.conn, wBuffer)
				if err != nil {
					if isClosedErr(err) {
						// Don't report this - we closed it
						return
					}

					c.logger.Infof("Client %s cannot read data to write: %s", c.name, err)
					return
				}

				if uint64(cn) != blocklen {
					c.logger.Infof("Client %s cannot read all data to write: %d != %d", c.name, cn, blocklen)
					return

				}
				wg.Add(1)
				go func(wBuffer []byte, offset int64, blocklen uint64, fua bool) {
					defer wg.Done()
					// WARNING: potential overflow (blocklen, offset)
					bn, err := c.backend.WriteAt(ctx, wBuffer, offset, fua)
					if err != nil {
						c.logger.Infof("Client %s got write I/O error: %s", c.name, err)
						nbdRep.NbdError = errorCodeFromGolangError(err)
					} else if uint64(bn) != blocklen {
						c.logger.Infof("Client %s got incomplete write (%d != %d) at offset %d", c.name, bn, blocklen, offset)
						nbdRep.NbdError = NBD_EIO
					}
				}(wBuffer, int64(offset), blocklen, fua && blocklen == length)
				length -= blocklen
				offset += blocklen

				blocklen = memoryBlockSize
				if blocklen > length {
					blocklen = length
				}
			}

			wg.Wait()

		case NBD_CMD_WRITE_ZEROES:
			var n int64
			var err error

			wg := sync.WaitGroup{}

			for blocklen > 0 {
				wg.Add(1)
				go func(offset int64, blocklen int64, fua bool) {
					defer wg.Done()
					n, err = c.backend.WriteZeroesAt(ctx, offset, blocklen, fua)
					if err != nil {
						c.logger.Infof("Client %s got write I/O error: %s", c.name, err)
						nbdRep.NbdError = errorCodeFromGolangError(err)
					} else if int64(n) != blocklen {
						c.logger.Infof("Client %s got incomplete write (%d != %d) at offset %d", c.name, n, blocklen, offset)
						nbdRep.NbdError = NBD_EIO
					}
				}(int64(offset), int64(blocklen), fua && blocklen == length)

				length -= blocklen
				offset += blocklen

				blocklen = memoryBlockSize
				if blocklen > length {
					blocklen = length
				}
			}

			wg.Wait()

		case NBD_CMD_FLUSH:
			if err := c.backend.Flush(ctx); err != nil {
				c.logger.Infof("Client %s got flush I/O error: %s", c.name, err)
				nbdRep.NbdError = errorCodeFromGolangError(err)
			}

		case NBD_CMD_TRIM:
			var n int64
			var err error

			wg := sync.WaitGroup{}

			for blocklen > 0 {
				wg.Add(1)
				go func(offset int64, blocklen int64) {
					defer wg.Done()
					n, err = c.backend.TrimAt(ctx, offset, blocklen)
					if err != nil {
						c.logger.Infof("Client %s got trim I/O error: %s", c.name, err)
						nbdRep.NbdError = errorCodeFromGolangError(err)
					} else if int64(n) != blocklen {
						c.logger.Infof("Client %s got incomplete trim (%d != %d) at offset %d", c.name, n, blocklen, offset)
						nbdRep.NbdError = NBD_EIO
					}
				}(int64(offset), int64(blocklen))

				length -= blocklen
				offset += blocklen

				blocklen = memoryBlockSize
				if blocklen > length {
					blocklen = length
				}
			}

			wg.Wait()

		case NBD_CMD_DISC:
			c.waitForInflight(ctx, 1) // this request is itself in flight, so 1 is permissible
			c.logger.Infof("Client %s requested disconnect\n", c.name)
			if err := c.backend.Flush(ctx); err != nil {
				c.logger.Infof("Client %s cannot flush backend: %s\n", c.name, err)
			}
			return

		default:
			c.logger.Infof("Client %s sent unknown command %d\n",
				c.name, req.NbdCommandType)
			return
		}

		if req.NbdCommandType != NBD_CMD_READ {
			if !c.sendHeader(ctx, nbdRep) {
				return
			}
		}

		// if we've recieved a disconnect, just sit waiting for the
		// context to indicate we've done
		if atomic.LoadInt64(&c.disconnectReceived) > 0 {
			select {
			case <-ctx.Done():
				return
			}
		}
	}
}

// kill a connection.
// This safely ensures the kill channel is closed if it isn't already, which will
// kill all the goroutines
func (c *Connection) kill(ctx context.Context) {
	c.killMutex.Lock()
	defer c.killMutex.Unlock()
	if !c.killed {
		close(c.killCh)
		c.killed = true
	}
}

func (c *Connection) waitForInflight(ctx context.Context, limit int64) {
	c.logger.Infof("Client %s waiting for inflight requests prior to disconnect", c.name)
	for {
		if atomic.LoadInt64(&c.numInflight) <= limit {
			return
		}
		// this is pretty nasty in that it would be nicer to wait on
		// a channel or use a (non-existent) waitgroup with timer.
		// however it's only one atomic read every 10ms and this
		// will hardly ever occur
		time.Sleep(10 * time.Millisecond)
	}
}

// Serve the two phases of an NBD connection.
// The first phase is the Negotiation between Server and Client.
// The second phase is the transmition of data, replies based on requests.
func (c *Connection) Serve(parentCtx context.Context) {
	ctx, cancelFunc := context.WithCancel(parentCtx)

	c.repCh = make(chan []byte, 1024)
	c.killCh = make(chan struct{})

	c.conn = c.plainConn
	c.name = c.plainConn.RemoteAddr().String()
	if c.name == "" {
		c.name = "[unknown]"
	}

	defer func() {
		if c.backend != nil {
			c.backend.Close(ctx)
		}
		if c.tlsConn != nil {
			c.tlsConn.Close()
		}
		c.plainConn.Close()
		cancelFunc()

		c.kill(ctx) // to ensure the kill channel is closed

		c.wg.Wait()
		close(c.repCh)

		c.logger.Infof("Closed connection from %s", c.name)
	}()

	// Phase #1: Negotiation
	if err := c.negotiate(ctx); err != nil {
		c.logger.Infof("Negotiation failed with %s: %v", c.name, err)
		return
	}

	c.name = fmt.Sprintf("%s/%s", c.name, c.export.name)

	c.logger.Infof("Negotiation succeeded with %s", c.name)

	// Phase #2: Transmition

	c.wg.Add(2)
	go c.receive(ctx)
	go c.reply(ctx)

	// Wait until either we are explicitly killed or one of our
	// workers dies
	select {
	case <-c.killCh:
		c.logger.Infof("Worker forced close for %s", c.name)
	case <-ctx.Done():
		c.logger.Infof("Parent forced close for %s", c.name)
	}
}

// negotiate a connection
func (c *Connection) negotiate(ctx context.Context) error {
	c.conn.SetDeadline(time.Now().Add(c.params.ConnectionTimeout))

	// We send a newstyle header
	nsh := nbdNewStyleHeader{
		NbdMagic:       NBD_MAGIC,
		NbdOptsMagic:   NBD_OPTS_MAGIC,
		NbdGlobalFlags: NBD_FLAG_FIXED_NEWSTYLE,
	}

	if !c.listener.disableNoZeroes {
		nsh.NbdGlobalFlags |= NBD_FLAG_NO_ZEROES
	}

	if err := binary.Write(c.conn, binary.BigEndian, nsh); err != nil {
		return errors.New("Cannot write magic header")
	}

	// next they send client flags
	var clf nbdClientFlags

	if err := binary.Read(c.conn, binary.BigEndian, &clf); err != nil {
		return errors.New("Cannot read client flags")
	}

	done := false
	// now we get options
	for !done {
		var opt nbdClientOpt
		if err := binary.Read(c.conn, binary.BigEndian, &opt); err != nil {
			return errors.New("Cannot read option (perhaps client dropped the connection)")
		}
		if opt.NbdOptMagic != NBD_OPTS_MAGIC {
			return errors.New("Bad option magic")
		}
		if opt.NbdOptLen > 65536 {
			return errors.New("Option is too long")
		}
		switch opt.NbdOptID {
		case NBD_OPT_EXPORT_NAME, NBD_OPT_INFO, NBD_OPT_GO:
			var name []byte

			clientSupportsBlockSizeConstraints := false

			if opt.NbdOptID == NBD_OPT_EXPORT_NAME {
				name = make([]byte, opt.NbdOptLen)
				n, err := io.ReadFull(c.conn, name)
				if err != nil {
					return err
				}
				if uint32(n) != opt.NbdOptLen {
					return errors.New("Incomplete name")
				}
			} else {
				var numInfoElements uint16
				if err := binary.Read(c.conn, binary.BigEndian, &numInfoElements); err != nil {
					return errors.New("Bad number of info elements")
				}
				for i := uint16(0); i < numInfoElements; i++ {
					var infoElement uint16
					if err := binary.Read(c.conn, binary.BigEndian, &infoElement); err != nil {
						return errors.New("Bad number of info elements")
					}
					switch infoElement {
					case NBD_INFO_BLOCK_SIZE:
						clientSupportsBlockSizeConstraints = true
					}
				}
				var nameLength uint32
				if err := binary.Read(c.conn, binary.BigEndian, &nameLength); err != nil {
					return errors.New("Bad export name length")
				}
				if nameLength > 4096 {
					return errors.New("Name is too long")
				}
				name = make([]byte, nameLength)
				n, err := io.ReadFull(c.conn, name)
				if err != nil {
					return err
				}
				if uint32(n) != nameLength {
					return errors.New("Incomplete name")
				}
				l := 2 + 2*uint32(numInfoElements) + 4 + uint32(nameLength)
				if opt.NbdOptLen > l {
					if err := skip(c.conn, opt.NbdOptLen-l); err != nil {
						return err
					}
				} else if opt.NbdOptLen < l {
					return errors.New("Option length too short")
				}
			}

			if len(name) == 0 {
				name = []byte(c.listener.defaultExport)
			}

			// Next find our export
			ec, err := c.listener.GetExportConfig(string(name))
			if err != nil || (ec.TLSOnly && c.tlsConn == nil) {
				if opt.NbdOptID == NBD_OPT_EXPORT_NAME {
					// we have to just abort here
					if err != nil {
						return err
					}
					return errors.New("Attempt to connect to TLS-only connection without TLS")
				}
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_ERR_UNKNOWN,
					NbdOptReplyLength: 0,
				}
				if err == nil {
					or.NbdOptReplyType = NBD_REP_ERR_TLS_REQD
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send info error")
				}
				break
			}

			// Now we know we are going to go with the export for sure
			// any failure beyond here and we are going to drop the
			// connection (assuming we aren't doing NBD_OPT_INFO)
			export, err := c.connectExport(ctx, ec)
			if err != nil {
				if opt.NbdOptID == NBD_OPT_EXPORT_NAME {
					return err
				}
				c.logger.Infof("Could not connect client %s to %s: %v", c.name, string(name), err)
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_ERR_UNKNOWN,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send info error")
				}
				break
			}

			// for the reply
			name = []byte(export.name)
			description := []byte(export.description)

			if opt.NbdOptID == NBD_OPT_EXPORT_NAME {
				// this option has a unique reply format
				ed := nbdExportDetails{
					NbdExportSize:  export.size,
					NbdExportFlags: export.exportFlags,
				}
				if err := binary.Write(c.conn, binary.BigEndian, ed); err != nil {
					return errors.New("Cannot write export details")
				}
			} else {
				// Send NBD_INFO_EXPORT
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: 12,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info export pt1")
				}
				ir := nbdInfoExport{
					NbdInfoType:          NBD_INFO_EXPORT,
					NbdExportSize:        export.size,
					NbdTransmissionFlags: export.exportFlags,
				}
				if err := binary.Write(c.conn, binary.BigEndian, ir); err != nil {
					return errors.New("Cannot write info export pt2")
				}

				// Send NBD_INFO_NAME
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: uint32(2 + len(name)),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info name pt1")
				}
				if err := binary.Write(c.conn, binary.BigEndian, uint16(NBD_INFO_NAME)); err != nil {
					return errors.New("Cannot write name id")
				}
				if err := binary.Write(c.conn, binary.BigEndian, name); err != nil {
					return errors.New("Cannot write name")
				}

				// Send NBD_INFO_DESCRIPTION
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: uint32(2 + len(description)),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info description pt1")
				}
				if err := binary.Write(c.conn, binary.BigEndian, uint16(NBD_INFO_DESCRIPTION)); err != nil {
					return errors.New("Cannot write description id")
				}
				if err := binary.Write(c.conn, binary.BigEndian, description); err != nil {
					return errors.New("Cannot write description")
				}

				// Send NBD_INFO_BLOCK_SIZE
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: 14,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info block size pt1")
				}
				ir2 := nbdInfoBlockSize{
					NbdInfoType:           NBD_INFO_BLOCK_SIZE,
					NbdMinimumBlockSize:   uint32(export.minimumBlockSize),
					NbdPreferredBlockSize: uint32(export.preferredBlockSize),
					NbdMaximumBlockSize:   uint32(export.maximumBlockSize),
				}
				if err := binary.Write(c.conn, binary.BigEndian, ir2); err != nil {
					return errors.New("Cannot write info block size pt2")
				}

				replyType := NBD_REP_ACK

				if export.minimumBlockSize > 1 && !clientSupportsBlockSizeConstraints {
					replyType = NBD_REP_ERR_BLOCK_SIZE_REQD
				}

				// Send ACK or error
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   replyType,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot info ack")
				}
				if opt.NbdOptID == NBD_OPT_INFO || or.NbdOptReplyType&NBD_REP_FLAG_ERROR != 0 {
					// Disassociate the backend as we are not closing
					c.backend.Close(ctx)
					c.backend = nil
					break
				}
			}

			if clf.NbdClientFlags&NBD_FLAG_C_NO_ZEROES == 0 && opt.NbdOptID == NBD_OPT_EXPORT_NAME {
				// send 124 bytes of zeroes.
				zeroes := make([]byte, 124, 124)
				if err := binary.Write(c.conn, binary.BigEndian, zeroes); err != nil {
					return errors.New("Cannot write zeroes")
				}
			}
			c.export = export
			done = true

		case NBD_OPT_LIST:
			names := c.listener.ListExportConfigNames()
			dedupMap := make(map[string]bool)
			var seen bool

			for _, name := range names {
				if _, seen = dedupMap[name]; seen {
					continue
				}

				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_SERVER,
					NbdOptReplyLength: uint32(len(name) + 4),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send list item")
				}
				l := uint32(len(name))
				if err := binary.Write(c.conn, binary.BigEndian, l); err != nil {
					return errors.New("Cannot send list name length")
				}
				if n, err := c.conn.Write([]byte(name)); err != nil || n != len(name) {
					return errors.New("Cannot send list name")
				}

				dedupMap[name] = true
			}
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptID:          opt.NbdOptID,
				NbdOptReplyType:   NBD_REP_ACK,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot send list ack")
			}
		case NBD_OPT_STARTTLS:
			if c.listener.tlsconfig == nil || c.tlsConn != nil {
				// say it's unsuppported
				c.logger.Infof("Rejecting upgrade of connection with %s to TLS", c.name)
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_ERR_UNSUP,
					NbdOptReplyLength: 0,
				}
				if c.tlsConn != nil { // TLS is already negotiated
					or.NbdOptReplyType = NBD_REP_ERR_INVALID
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot reply to unsupported TLS option")
				}
			} else {
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptID:          opt.NbdOptID,
					NbdOptReplyType:   NBD_REP_ACK,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send TLS ack")
				}
				c.logger.Infof("Upgrading connection with %s to TLS", c.name)
				// switch over to TLS
				tls := tls.Server(c.conn, c.listener.tlsconfig)
				c.tlsConn = tls
				c.conn = tls
				// explicitly handshake so we get an error here if there is an issue
				if err := tls.Handshake(); err != nil {
					return fmt.Errorf("TLS handshake failed: %s", err)
				}
			}
		case NBD_OPT_ABORT:
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptID:          opt.NbdOptID,
				NbdOptReplyType:   NBD_REP_ACK,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot send abort ack")
			}
			return errors.New("Connection aborted by client")
		default:
			// eat the option
			if err := skip(c.conn, opt.NbdOptLen); err != nil {
				return err
			}
			// say it's unsuppported
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptID:          opt.NbdOptID,
				NbdOptReplyType:   NBD_REP_ERR_UNSUP,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot reply to unsupported option")
			}
		}
	}

	c.conn.SetDeadline(time.Time{})
	return nil
}

// skip bytes
func skip(r io.Reader, n uint32) error {
	for n > 0 {
		l := n
		if l > 1024 {
			l = 1024
		}
		b := make([]byte, l)
		if nr, err := io.ReadFull(r, b); err != nil {
			return err
		} else if nr != int(l) {
			return errors.New("skip returned short read")
		}
		n -= l
	}
	return nil
}

// round a uint64 up to the next power of two
func roundUpToNextPowerOfTwo(x uint64) uint64 {
	var r uint64 = 1
	for i := 0; i < 64; i++ {
		if x <= r {
			return r
		}
		r = r << 1
	}
	return 0 // won't fit in uint64 :-(
}

// connectExport generates an export for a given name, and connects to it using the chosen backend
func (c *Connection) connectExport(ctx context.Context, ec *ExportConfig) (*Export, error) {
	// defaults to false in case of error,
	// this is good enough for our purposes
	forceFlush, _ := strconv.ParseBool(ec.DriverParameters["flush"])
	forceFua, _ := strconv.ParseBool(ec.DriverParameters["fua"])

	backendgen, ok := backendMap[strings.ToLower(ec.Driver)]
	if !ok {
		return nil, fmt.Errorf("No such driver %s", ec.Driver)
	}

	backend, err := backendgen(ctx, ec)
	if err != nil {
		return nil, err
	}

	gem, err := backend.Geometry(ctx)
	if err != nil {
		backend.Close(ctx)
		return nil, err
	}
	if c.backend != nil {
		c.backend.Close(ctx)
	}
	c.backend = backend

	if ec.MinimumBlockSize != 0 {
		gem.MinimumBlockSize = ec.MinimumBlockSize
	}
	if ec.PreferredBlockSize != 0 {
		gem.PreferredBlockSize = ec.PreferredBlockSize
	}
	if ec.MaximumBlockSize != 0 {
		gem.MaximumBlockSize = ec.MaximumBlockSize
	}
	if gem.MinimumBlockSize == 0 {
		gem.MinimumBlockSize = 1
	}
	gem.MinimumBlockSize = roundUpToNextPowerOfTwo(gem.MinimumBlockSize)
	gem.PreferredBlockSize = roundUpToNextPowerOfTwo(gem.PreferredBlockSize)
	// ensure preferredBlockSize is a multiple of the minimum block size
	gem.PreferredBlockSize = gem.PreferredBlockSize & ^(gem.MinimumBlockSize - 1)
	if gem.PreferredBlockSize < gem.MinimumBlockSize {
		gem.PreferredBlockSize = gem.MinimumBlockSize
	}
	// ensure maximumBlockSize is a multiple of preferredBlockSize
	gem.MaximumBlockSize = gem.MaximumBlockSize & ^(gem.PreferredBlockSize - 1)
	if gem.MaximumBlockSize < gem.PreferredBlockSize {
		gem.MaximumBlockSize = gem.PreferredBlockSize
	}

	flags := uint16(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_WRITE_ZEROES | NBD_FLAG_SEND_CLOSE)
	if backend.HasFua(ctx) || forceFua {
		flags |= NBD_FLAG_SEND_FUA
	}
	if backend.HasFlush(ctx) || forceFlush {
		flags |= NBD_FLAG_SEND_FLUSH
	}

	gem.Size = gem.Size & ^(gem.MinimumBlockSize - 1)
	return &Export{
		size:               gem.Size,
		exportFlags:        flags,
		name:               ec.Name,
		readonly:           ec.ReadOnly,
		tlsonly:            ec.TLSOnly,
		description:        ec.Description,
		minimumBlockSize:   gem.MinimumBlockSize,
		preferredBlockSize: gem.PreferredBlockSize,
		maximumBlockSize:   gem.MaximumBlockSize,
		memoryBlockSize:    gem.PreferredBlockSize,
	}, nil
}

// RegisterBackend allows you to register a backend with a name,
// overwriting any existing backend for that name
func RegisterBackend(name string, generator BackendGenerator) {
	backendMap[name] = generator
}

// ContainsBackend allows you to check if a backend is already available,
// even though you can still overwrite it with `RegisterBackend` in case you want.
func ContainsBackend(name string) bool {
	for k := range backendMap {
		if k == name {
			return true
		}
	}

	return false
}

// GetBackendNames returns the names of all registered backends
func GetBackendNames() []string {
	b := make([]string, len(backendMap))
	i := 0
	for k := range backendMap {
		b[i] = k
		i++
	}
	sort.Strings(b)
	return b
}
