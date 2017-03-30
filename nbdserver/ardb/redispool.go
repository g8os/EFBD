package ardb

import (
	"sync"
	"time"

	"github.com/g8os/blockstor/nbdserver/stubs"
	"github.com/garyburd/redigo/redis"
)

//RedisPool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The normal redigo.Pool is not adequate since it only maintains connections for a single server.
type RedisPool struct {
	lock        sync.Mutex //protects following
	connections map[string]*redis.Pool

	Dial func(connectionString string) (redis.Conn, error)
}

//NewRedisPool creates a new pool for multiple redis servers
func NewRedisPool(inMemory bool) (p *RedisPool) {
	p = &RedisPool{connections: make(map[string]*redis.Pool)}
	if inMemory {
		inMemoryRedisConnection := stubs.NewMemoryRedisConn()
		p.Dial = func(connectionString string) (redis.Conn, error) {
			return inMemoryRedisConnection, nil
		}
	} else {
		p.Dial = func(connectionString string) (redis.Conn, error) {
			return redis.Dial("tcp", connectionString)
		}
	}
	return
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *RedisPool) Get(connectionString string) redis.Conn {
	return p.GetConnectionSpecificPool(connectionString).Get()
}

// GetConnectionSpecificPool get a redis.Pool for a specific connectionString.
func (p *RedisPool) GetConnectionSpecificPool(connectionString string) (singleServerPool *redis.Pool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	singleServerPool = p.connections[connectionString]
	if singleServerPool != nil {
		return
	}
	singleServerPool = &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return p.Dial(connectionString) },
	}
	p.connections[connectionString] = singleServerPool
	return
}

// Close releases the resources used by the pool.
func (p *RedisPool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, c := range p.connections {
		c.Close()
	}
	p.connections = make(map[string]*redis.Pool)
}
