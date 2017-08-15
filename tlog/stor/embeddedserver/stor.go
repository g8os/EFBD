package embeddedserver

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/zero-os/0-stor/server/storserver"
)

type ZeroStorServer struct {
	dir    string
	server storserver.StoreServer
}

func NewZeroStorServer() (*ZeroStorServer, error) {
	tmpDir, err := ioutil.TempDir("", "embedzerostor")
	if err != nil {
		return nil, err
	}

	server, err := storserver.NewGRPC(filepath.Join(tmpDir, "data"), filepath.Join(tmpDir, "meta"))
	if err != nil {
		return nil, err
	}

	if _, err := server.Listen("localhost:0"); err != nil {
		return nil, err
	}

	return &ZeroStorServer{
		server: server,
		dir:    tmpDir,
	}, nil
}

func (zss *ZeroStorServer) Addr() string {
	return zss.server.Addr()
}

func (zss *ZeroStorServer) Close() error {
	zss.server.Close()
	return os.RemoveAll(zss.dir)
}
