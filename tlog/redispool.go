package tlog

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisPool maintains a collection of premade redis.Pool's,
// one pool per predefined connection.
// The application calls the Get method with a valid index
// to get a connection from the pool and the connection's Close method
// to return the connection's resources back to the pool.
//
// The normal redigo.Pool is not adequate since it only maintains connections for a single server.
type RedisPool struct {
	lock          sync.Mutex //protects following
	pools         map[int]*redis.Pool
	numberOfPools int
}

// NewRedisPool creates a new pool for multiple redis servers,
// the number of pools defined by the number of given connection strings.
func NewRedisPool(dialConnectionStrings []string) (*RedisPool, error) {
	numberOfPools := len(dialConnectionStrings)
	if numberOfPools == 0 {
		return nil, errors.New("at least one dial connection string is required to create a RedisPool")
	}

	pools := make(map[int]*redis.Pool, numberOfPools)
	for index := range dialConnectionStrings {
		addr := dialConnectionStrings[index]
		pools[index] = &redis.Pool{
			MaxActive:   3,
			MaxIdle:     3,
			Wait:        true,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", addr)
			},
		}
	}

	return &RedisPool{
		pools:         pools,
		numberOfPools: numberOfPools,
	}, nil
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *RedisPool) Get(index int) redis.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	if pool, ok := p.pools[index]; ok {
		return pool.Get()
	}

	return errorConnection{
		err: fmt.Errorf("no connectionPool exists for index %d", index),
	}
}

// Close releases the resources used by the pool.
func (p *RedisPool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, c := range p.pools {
		c.Close()
	}

	p.pools, p.numberOfPools = nil, 0
}

// errorConnection taken from
// https://github.com/garyburd/redigo/blob/ac91d6ff49bd0d278a90201de77a4f8ad9628e25/redis/pool.go#L409-L416
type errorConnection struct{ err error }

func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConnection) Err() error                                     { return ec.err }
func (ec errorConnection) Close() error                                   { return ec.err }
func (ec errorConnection) Flush() error                                   { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }
