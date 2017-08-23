package embedserver

import (
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
)

// Server is embedded metadata server
// which listen on unix socket
type Server struct {
	lcDir      string
	lpDir      string
	etcd       *embed.Etcd
	listenAddr string
}

// Stop stops the server and release it's resources
func (s *Server) Stop() {
	s.etcd.Server.Stop()
	<-s.etcd.Server.StopNotify()
	s.etcd.Close()
	os.RemoveAll(s.lpDir)
	os.RemoveAll(s.lcDir)
}

// ListenAddrs returns listen address of this server
func (s *Server) ListenAddr() string {
	return s.listenAddr
}

// New creates new embeddded metadata server
func New() (*Server, error) {
	tmpDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		return nil, err
	}

	cfg := embed.NewConfig()
	cfg.Dir = tmpDir

	capnslog.SetGlobalLogLevel(capnslog.CRITICAL)
	// listen client URL
	// we use tmpDir as unix address because it is a simple
	// yet valid way to generate random string
	lcurl, err := url.Parse("unix://" + filepath.Base(tmpDir))
	if err != nil {
		return nil, err
	}
	cfg.LCUrls = []url.URL{*lcurl}

	// listen peer url
	// same strategy with listen client URL
	lpDir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		return nil, err
	}
	lpurl, err := url.Parse("unix://" + filepath.Base(lpDir))
	if err != nil {
		return nil, err
	}
	cfg.LPUrls = []url.URL{*lpurl}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	<-e.Server.ReadyNotify()

	conf := e.Config()

	return &Server{
		lpDir:      lpDir,
		lcDir:      tmpDir,
		etcd:       e,
		listenAddr: conf.LCUrls[0].String(),
	}, nil
}
