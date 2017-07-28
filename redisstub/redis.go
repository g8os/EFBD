// Package redisstub is a minimal package
// providing redis-related (in-memory) implementations
// meant for testing and dev purposes only.
package redisstub

import (
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"
	"github.com/zero-os/0-Disk/log"
)

// NewMemoryRedis creates a new in-memory redis stub.
// It must be noted that the stub only partially redis-compliant,
// not all commands (such as MULTI/EXEC) are supported.
// All available commands can be found at:
// https://github.com/siddontang/ledisdb/blob/master/doc/commands.md
// WARNING: should be used for testing/dev purposes only!
func NewMemoryRedis() *MemoryRedis {
	cfg := config.NewConfigDefault()
	cfg.DBName = "memory"
	cfg.DataDir, _ = ioutil.TempDir("", "redisstub")
	// assigning the empty string to Addr,
	// such that it auto-assigns a free local port
	cfg.Addr = ""

	app, err := server.NewApp(cfg)
	if err != nil {
		log.Fatalf("couldn't create embedded ledisdb: %s", err.Error())
	}

	return &MemoryRedis{
		app:     app,
		addr:    app.Address(),
		datadir: cfg.DataDir,
	}
}

//MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	app     *server.App
	addr    string
	datadir string
}

// Listen to any incoming TCP requests,
// and process them in the embedded Go Redis Server.
func (mr *MemoryRedis) Listen() {
	if mr == nil {
		return
	}

	log.Info("embedded LedisDB Server ready and listening at ", mr.addr)
	mr.app.Run()
}

// Dial to the embedded Go Redis Server,
// and return the established connection if possible.
func (mr *MemoryRedis) Dial(connectionString string, database int) (redis.Conn, error) {
	if mr == nil {
		return nil, errors.New("no in-memory redis is available")
	}

	return redis.Dial("tcp", mr.addr, redis.DialDatabase(database))
}

// Close the embedded Go Redis Server,
// and delete the used datadir.
func (mr *MemoryRedis) Close() {
	if mr == nil {
		return
	}

	os.Remove(mr.datadir)
	mr.app.Close()
}

// Address returns the tcp (local) address of this MemoryRedis server
func (mr *MemoryRedis) Address() string {
	return mr.addr
}

// NewInMemoryRedisProvider returns an ARDB Connection Provider,
// which uses an in-memory ARDB for all its purposes.
// See documentation for NewMemoryRedis more information.
// WARNING: should be used for testing/dev purposes only!
func NewInMemoryRedisProvider(template *InMemoryRedisProvider) *InMemoryRedisProvider {
	provider := new(InMemoryRedisProvider)

	provider.memRedis = NewMemoryRedis()
	go provider.memRedis.Listen()

	provider.primaryPool = newInMemoryRedisPool(func() (redis.Conn, error) {
		return provider.memRedis.Dial("", 0)
	})

	provider.templatePool = newInMemoryRedisPool(func() (redis.Conn, error) {
		return provider.templateMemRedis.Dial("", 0)
	})

	if template != nil {
		provider.templateMemRedis = template.memRedis
	}

	return provider
}

// InMemoryRedisProvider provides a in memory provider
// for any redis connection.
// While it is safe to create this struct directly,
// it is recommended to create it using NewInMemoryRedisProvider.
// WARNING: should be used for testing/dev purposes only!
type InMemoryRedisProvider struct {
	memRedis                  *MemoryRedis
	templateMemRedis          *MemoryRedis
	primaryPool, templatePool *redis.Pool
}

// DataConnection implements ConnProvider.DataConnection
func (rp *InMemoryRedisProvider) DataConnection(index int64) (redis.Conn, error) {
	return rp.primaryPool.Get(), nil
}

// MetadataConnection implements ConnProvider.MetadataConnection
func (rp *InMemoryRedisProvider) MetadataConnection() (redis.Conn, error) {
	return rp.primaryPool.Get(), nil
}

// TemplateConnection implements ConnProvider.TemplateConnection
func (rp *InMemoryRedisProvider) TemplateConnection(index int64) (redis.Conn, error) {
	return rp.templatePool.Get(), nil
}

// Close implements ConnProvider.Close
func (rp *InMemoryRedisProvider) Close() error {
	rp.memRedis.Close()
	rp.templateMemRedis.Close()
	return nil
}

// SetTemplatePool allows you to set the template source for this provider,
// using the primary source of the given provider.
// The template source of this provider will be unset in case nil is passed in as an argument.
func (rp *InMemoryRedisProvider) SetTemplatePool(template *InMemoryRedisProvider) {
	if template == nil {
		rp.templateMemRedis = nil
		return
	}

	rp.templateMemRedis = template.memRedis
}

func newInMemoryRedisPool(dial func() (redis.Conn, error)) *redis.Pool {
	return &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 5 * time.Second,
		Dial:        dial,
	}
}
