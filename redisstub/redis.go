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
	lediscfg "github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// NewMemoryRedis creates a new in-memory redis stub.
// It must be noted that the stub only partially redis-compliant,
// not all commands (such as MULTI/EXEC) are supported.
// All available commands can be found at:
// https://github.com/siddontang/ledisdb/blob/master/doc/commands.md
// WARNING: should be used for testing/dev purposes only!
func NewMemoryRedis() *MemoryRedis {
	cfg := lediscfg.NewConfigDefault()
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
	if mr == nil {
		return ""
	}

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
func (rp *InMemoryRedisProvider) DataConnection(index int64) (ardb.Connection, error) {
	return newInMemoryARDBConnection(rp.primaryPool, rp.memRedis), nil
}

// MetadataConnection implements ConnProvider.MetadataConnection
func (rp *InMemoryRedisProvider) MetadataConnection() (ardb.Connection, error) {
	return newInMemoryARDBConnection(rp.primaryPool, rp.memRedis), nil
}

// TemplateConnection implements ConnProvider.TemplateConnection
func (rp *InMemoryRedisProvider) TemplateConnection(index int64) (ardb.Connection, error) {
	if rp.templateMemRedis == nil {
		return nil, ardb.ErrTemplateClusterNotSpecified
	}
	if rp.templatePool == nil {
		return nil, ardb.ErrServerMarkedInvalid
	}

	return newInMemoryARDBConnection(rp.templatePool, rp.templateMemRedis), nil
}

// MarkTemplateConnectionInvalid implements ConnProvider.MarkTemplateConnectionInvalid
func (rp *InMemoryRedisProvider) MarkTemplateConnectionInvalid(index int64) {
	rp.templatePool = nil
}

// PrimaryAddress returns the address of the primary in-memory ardb server.
func (rp *InMemoryRedisProvider) PrimaryAddress() string {
	return rp.memRedis.Address()
}

// TemplateAddress returns the address of the template in-memory ardb server,
// or returns false if no template is defined instead.
func (rp *InMemoryRedisProvider) TemplateAddress() (string, bool) {
	if rp.templateMemRedis == nil {
		return "", false
	}

	return rp.templateMemRedis.Address(), true
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

func newInMemoryARDBConnection(pool *redis.Pool, mr *MemoryRedis) ardb.Connection {
	return &inMemoryARDBConnection{
		Conn:    pool.Get(),
		address: mr.Address(),
	}
}

type inMemoryARDBConnection struct {
	redis.Conn
	address string
}

// ConnectionConfig implements Connection.ConnectionConfig
func (conn *inMemoryARDBConnection) ConnectionConfig() config.StorageServerConfig {
	return config.StorageServerConfig{
		Address: conn.address,
	}
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

// NewInMemoryRedisProviderMultiServers returns an ARDB Connection Provider,
// which uses multiple in-memory ARDB for all its purposes.
// See documentation for NewMemoryRedis more information.
// WARNING: should be used for testing/dev purposes only!
func NewInMemoryRedisProviderMultiServers(dataServers, templateServers int) *InMemoryRedisProviderMultiServers {
	if dataServers < 1 {
		panic("no data servers given, while we require at least one")
	}

	var memRedisSlice []*MemoryRedis
	for i := 0; i < dataServers; i++ {
		memRedis := NewMemoryRedis()
		go memRedis.Listen()
		memRedisSlice = append(memRedisSlice, memRedis)
	}

	var templateMemRedisSlice []*MemoryRedis
	for i := 0; i < templateServers; i++ {
		memRedis := NewMemoryRedis()
		go memRedis.Listen()
		templateMemRedisSlice = append(templateMemRedisSlice, memRedis)
	}

	return &InMemoryRedisProviderMultiServers{
		memRedisSlice:         memRedisSlice,
		templateMemRedisSlice: templateMemRedisSlice,
	}
}

// InMemoryRedisProviderMultiServers provides a in memory provider
// for any redis connection, using multiple in-memory ardb servers.
// While it is safe to create this struct directly,
// it is recommended to create it using NewInMemoryRedisProviderMultiServers.
// WARNING: should be used for testing/dev purposes only!
type InMemoryRedisProviderMultiServers struct {
	memRedisSlice         []*MemoryRedis
	templateMemRedisSlice []*MemoryRedis
}

// DataConnection implements ConnProvider.DataConnection
func (rp *InMemoryRedisProviderMultiServers) DataConnection(index int64) (ardb.Connection, error) {
	i := index % int64(len(rp.memRedisSlice))
	addr := rp.memRedisSlice[i].Address()
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &inMemoryARDBConnection{conn, addr}, nil

}

// MetadataConnection implements ConnProvider.MetadataConnection
func (rp *InMemoryRedisProviderMultiServers) MetadataConnection() (ardb.Connection, error) {
	for _, memRedis := range rp.memRedisSlice {
		addr := memRedis.Address()
		conn, err := redis.Dial("tcp", addr)
		if err == nil {
			return &inMemoryARDBConnection{conn, addr}, nil
		}
	}

	return nil, errors.New("no storage server available")
}

// TemplateConnection implements ConnProvider.TemplateConnection
func (rp *InMemoryRedisProviderMultiServers) TemplateConnection(index int64) (ardb.Connection, error) {
	n := len(rp.templateMemRedisSlice)
	if n < 1 {
		return nil, ardb.ErrTemplateClusterNotSpecified
	}

	i := index % int64(n)
	memRedis := rp.templateMemRedisSlice[i]
	if memRedis == nil {
		return nil, ardb.ErrServerMarkedInvalid
	}

	addr := memRedis.Address()
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &inMemoryARDBConnection{conn, addr}, nil
}

// MarkTemplateConnectionInvalid implements ConnProvider.MarkTemplateConnectionInvalid
func (rp *InMemoryRedisProviderMultiServers) MarkTemplateConnectionInvalid(index int64) {
	n := len(rp.templateMemRedisSlice)
	if n < 1 {
		return
	}

	i := index % int64(n)
	rp.templateMemRedisSlice[i].Close()
	rp.templateMemRedisSlice[i] = nil
}

// ClusterConfig returns the cluster config which
// can be used to connect to the storage cluster which underlies this provider.
func (rp *InMemoryRedisProviderMultiServers) ClusterConfig() *config.StorageClusterConfig {
	cluster := new(config.StorageClusterConfig)

	for _, memRedis := range rp.memRedisSlice {
		cluster.Servers = append(cluster.Servers, config.StorageServerConfig{
			Address: memRedis.Address(),
		})
	}

	return cluster
}

// Close implements ConnProvider.Close
func (rp *InMemoryRedisProviderMultiServers) Close() error {
	for _, memRedis := range rp.memRedisSlice {
		memRedis.Close()
	}
	for _, memRedis := range rp.templateMemRedisSlice {
		if memRedis == nil {
			continue
		}
		memRedis.Close()
	}
	return nil
}
