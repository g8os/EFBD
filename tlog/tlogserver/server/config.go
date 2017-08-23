package server

import (
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

// DefaultConfig creates a new config, using sane defaults
func DefaultConfig() *Config {
	return &Config{
		K:          4,
		M:          2,
		ListenAddr: "0.0.0.0:11211",
		FlushSize:  25,
		FlushTime:  25,
		BlockSize:  4096,
		PrivKey:    "12345678901234567890123456789012",
	}
}

// Config used for creating the tlogserver
type Config struct {
	K          int
	M          int
	BlockSize  int // size of each block, used as hint for the flusher buffer size
	ListenAddr string
	FlushSize  int
	FlushTime  int
	PrivKey    string
	AggMq      *aggmq.MQ
}

// RequiredDataServers returns how many data servers are required,
// based on the K and M values.
func (conf *Config) RequiredDataServers() int {
	return conf.K + conf.M
}

// flusherConfig is used by the server to create a flusher
// for a specific vdisk.
type flusherConfig struct {
	K         int
	M         int
	FlushSize int
	FlushTime int
	PrivKey   string
}
