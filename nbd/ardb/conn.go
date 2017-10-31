package ardb

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
)

// StandardConnectionDialer defines a non-pooled standard connection dialer.
type StandardConnectionDialer struct{}

// Dial implements ConnetionDialer.Dial
func (scd StandardConnectionDialer) Dial(cfg config.StorageServerConfig) (Conn, error) {
	return Dial(cfg)
}

// NewPool creates a new pool for multiple ardb servers,
// if no dialFunc is given, a default one will be used instead,
// which established a tcp connection for the given connection info.
func NewPool(dial DialFunc) *Pool {
	if dial == nil {
		dial = Dial
	}

	return &Pool{
		pools:    make(map[config.StorageServerConfig]*redis.Pool),
		dialFunc: dial,
	}
}

// Pool maintains a collection of pools (one pool per config).
type Pool struct {
	mux      sync.Mutex //protects following
	pools    map[config.StorageServerConfig]*redis.Pool
	dialFunc DialFunc
}

// Dial implements ConnectionDialer.Dial
func (p *Pool) Dial(cfg config.StorageServerConfig) (Conn, error) {
	if cfg.State != config.StorageServerStateOnline {
		return nil, ErrServerUnavailable
	}

	p.mux.Lock()
	pool, err := p.getConnectionSpecificPool(cfg)
	p.mux.Unlock()
	if err != nil {
		return nil, err
	}

	conn := pool.Get()
	return conn, conn.Err()
}

// GetConnectionSpecificPool gets a redis.Pool for a specific config.
func (p *Pool) getConnectionSpecificPool(cfg config.StorageServerConfig) (*redis.Pool, error) {
	if p.pools == nil {
		return nil, errPoolAlreadyClosed
	}

	pool, ok := p.pools[cfg]
	if ok {
		return pool, nil
	}

	pool = &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return p.dialFunc(cfg) },
	}
	p.pools[cfg] = pool
	return pool, nil
}

// Close releases the resources used by the pool.
func (p *Pool) Close() {
	p.mux.Lock()
	defer p.mux.Unlock()

	// close all storage server pools
	for _, pool := range p.pools {
		pool.Close()
	}

	p.pools = nil
}

// ConnectionDialer defines a type which allows you to dial a connection.
type ConnectionDialer interface {
	// Dial an ARDB connection.
	// The callee must close the returned connection.
	Dial(cfg config.StorageServerConfig) (Conn, error)
}

// Dial a standard (TCP) connection using a given ARDB server config.
func Dial(cfg config.StorageServerConfig) (Conn, error) {
	if cfg.State != config.StorageServerStateOnline {
		return nil, ErrServerUnavailable
	}
	return redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
}

// DialAll dials standard (TCP) connections for the given ARDB server configs.
func DialAll(cfgs ...config.StorageServerConfig) (conns []Conn, err error) {
	if len(cfgs) == 0 {
		return nil, ErrInvalidInput
	}

	var conn Conn
	for _, cfg := range cfgs {
		// dial a single connection
		conn, err = Dial(cfg)
		if err != nil {
			// connecton failed, close all open connections and return it all
			var closeErr error
			for _, conn = range conns {
				closeErr = conn.Close()
				if closeErr != nil {
					log.Errorf("couldn't close open connection: %v", err)
				}
			}

			return
		}

		// connection established, add it to the list of open connections
		conns = append(conns, conn)
	}

	return
}

// DialFunc represents any kind of function,
// used to dial an ARDB connection.
type DialFunc func(cfg config.StorageServerConfig) (Conn, error)

// Conn represents a connection to an ARDB server.
type Conn interface {
	// Close closes the connection.
	Close() error

	// Err returns a non-nil value when the connection is not usable.
	Err() error

	// Do sends a command to the server and returns the received reply.
	Do(commandName string, args ...interface{}) (reply interface{}, err error)

	// Send writes the command to the client's output buffer.
	Send(commandName string, args ...interface{}) error

	// Flush flushes the output buffer to the ARDB server.
	Flush() error

	// Receive receives a single reply from the ARDB server
	Receive() (reply interface{}, err error)
}

// Various connection-related errors returned by this package.
var (
	errAddressNotGiven   = errors.New("ARDB server address not given")
	errPoolAlreadyClosed = errors.New("ARDB pool already closed")
)
