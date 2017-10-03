package embeddedserver

import (
	"io/ioutil"
	"os"
	"path/filepath"

	badgerkv "github.com/dgraph-io/badger"
	"github.com/zero-os/0-stor/server"
	"github.com/zero-os/0-stor/server/db/badger"
)

type zeroStorServer struct {
	dir    string
	server server.StoreServer
}

func newZeroStorServer() (*zeroStorServer, error) {
	tmpDir, err := ioutil.TempDir("", "embedzerostor")
	if err != nil {
		return nil, err
	}

	opts := badgerkv.DefaultOptions
	opts.MaxTableSize = 64 << 5
	db, err := badger.NewWithOpts(filepath.Join(tmpDir, "data"), filepath.Join(tmpDir, "meta"), opts)
	if err != nil {
		return nil, err
	}

	server, err := server.NewWithDB(db, false, 16)
	if err != nil {
		return nil, err
	}

	if _, err := server.Listen("localhost:0"); err != nil {
		return nil, err
	}

	return &zeroStorServer{
		server: server,
		dir:    tmpDir,
	}, nil
}

func (zss *zeroStorServer) Addr() string {
	return zss.server.Addr()
}

func (zss *zeroStorServer) Close() error {
	zss.server.Close()
	return os.RemoveAll(zss.dir)
}
