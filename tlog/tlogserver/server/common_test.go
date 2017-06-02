package server

import (
	"testing"

	"github.com/zero-os/0-Disk/tlog"
)

func createTestServer(t *testing.T, flushTime int) (*Server, *Config, error) {
	conf := DefaultConfig()
	conf.ListenAddr = "127.0.0.1:0"
	if flushTime > 0 {
		conf.FlushTime = flushTime
	}

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(conf.RequiredDataServers())

	s, err := NewServer(conf, poolFactory)
	return s, conf, err
}
