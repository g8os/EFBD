package redisstub

import (
	"io/ioutil"
	"os"

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
	log.Info("embedded LedisDB Server ready and listening at ", mr.addr)
	mr.app.Run()
}

// Dial to the embedded Go Redis Server,
// and return the established connection if possible.
func (mr *MemoryRedis) Dial(connectionString string, database int) (redis.Conn, error) {
	return redis.Dial("tcp", mr.addr, redis.DialDatabase(database))
}

// Close the embedded Go Redis Server,
// and delete the used datadir.
func (mr *MemoryRedis) Close() {
	os.Remove(mr.datadir)
	mr.app.Close()
}

// Address returns the tcp (local) address of this MemoryRedis server
func (mr *MemoryRedis) Address() string {
	return mr.addr
}
