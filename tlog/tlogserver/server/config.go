package server

import (
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

const (
	// WaitListenAddrRandom is wait listen addr which wait in random port.
	// Only useful for test
	WaitListenAddrRandom = "*"
)

// DefaultConfig creates a new config, using sane defaults
func DefaultConfig() *Config {
	return &Config{
		ListenAddr: "0.0.0.0:11211",
		FlushSize:  flusher.DefaultFlushSize,
		FlushTime:  25,
		BlockSize:  4096,
		PrivKey:    "12345678901234567890123456789012",
	}
}

// Config used for creating the tlogserver
type Config struct {
	BlockSize       int // size of each block, used as hint for the flusher buffer size
	ListenAddr      string
	AcceptAddr      string // address from which the tlog server can accept connection from
	FlushSize       int
	FlushTime       int
	PrivKey         string
	AggMq           *aggmq.MQ
	WaitListenAddr  string
	WaitConnectAddr string
}

// flusherConfig is used by the server to create a flusher
// for a specific vdisk.
type flusherConfig struct {
	FlushSize int
	FlushTime int
	PrivKey   string
}
