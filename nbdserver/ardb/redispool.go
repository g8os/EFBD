package ardb

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// DialFunc creates a redis.Conn (if possible),
// based on a given connectionString and database.
type DialFunc func(connectionString string, database int) (redis.Conn, error)

//RedisPool maintains a collection of redis.Pool's per connection's database.
// The application calls the Get method
// to get a database connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The normal redigo.Pool is not adequate since it only maintains connections for a single server.
type RedisPool struct {
	lock                    sync.Mutex //protects following
	connectionSpecificPools map[string]databaseSpecificPools

	Dial DialFunc
}

type databaseSpecificPools map[int]*redis.Pool

//NewRedisPool creates a new pool for multiple redis servers
func NewRedisPool(dial DialFunc) (p *RedisPool) {
	p = &RedisPool{connectionSpecificPools: make(map[string]databaseSpecificPools)}
	if dial == nil {
		p.Dial = func(connectionString string, database int) (redis.Conn, error) {
			return redis.Dial("tcp", connectionString, redis.DialDatabase(database))
		}
	} else {
		p.Dial = dial
	}

	return
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *RedisPool) Get(connectionString string, database int) redis.Conn {
	return p.getConnectionSpecificPool(connectionString, database).Get()
}

// GetConnectionSpecificPool gets a redis.Pool for a specific connectionString.
func (p *RedisPool) getConnectionSpecificPool(connectionString string, database int) (singleServerPool *redis.Pool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// get an existing or create a new redis pool map for a given connection string
	singleConnectionPool, ok := p.connectionSpecificPools[connectionString]
	if ok {
		singleServerPool = singleConnectionPool[database]
		if singleServerPool != nil {
			return
		}
	} else {
		singleConnectionPool = make(databaseSpecificPools)
		p.connectionSpecificPools[connectionString] = singleConnectionPool
	}

	// get an existing storage server pool
	if singleServerPool, ok = singleConnectionPool[database]; ok {
		return
	}

	// create a new storage server pool
	singleServerPool = &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return p.Dial(connectionString, database) },
	}
	singleConnectionPool[database] = singleServerPool
	return
}

// Close releases the resources used by the pool.
func (p *RedisPool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, connection := range p.connectionSpecificPools {
		for _, c := range connection {
			c.Close()
		}
	}
	p.connectionSpecificPools = make(map[string]databaseSpecificPools)
}
