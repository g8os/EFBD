package ledisdb

import (
	"io/ioutil"
	"os"

	lediscfg "github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"
	"github.com/zero-os/0-Disk/log"
)

// NewServer creates a new in-memory ledisdb server.
// It must be noted that the server is only partially redis-compliant,
// not all commands (such as MULTI/EXEC) are supported.
// All available commands can be found at:
// https://github.com/siddontang/ledisdb/blob/master/doc/commands.md
// WARNING: should be used for testing/dev purposes only!
func NewServer() *Server {
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

	server := &Server{
		app:     app,
		addr:    app.Address(),
		datadir: cfg.DataDir,
	}
	go server.listen()
	return server
}

// Server is an inmemory ledisdb server implementation
type Server struct {
	app     *server.App
	addr    string
	datadir string
}

// Close the embedded Go Redis Server,
// and delete the used datadir.
func (server *Server) Close() {
	if server == nil {
		return
	}

	os.Remove(server.datadir)
	server.app.Close()
}

// Address returns the tcp (local) address of this MemoryRedis server
func (server *Server) Address() string {
	if server == nil {
		return ""
	}

	return server.addr
}

// listen to any incoming TCP requests,
// and process them in the embedded Go Redis Server.
func (server *Server) listen() {
	log.Info("embedded LedisDB Server ready and listening at ", server.addr)
	server.app.Run()
}
