package server

import (
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

// DefaultConfig creates a new config, using sane defaults
func DefaultConfig() *Config {
	return &Config{
		DataShards:   4,
		ParityShards: 2,
		ListenAddr:   "0.0.0.0:11211",
		FlushSize:    flusher.DefaultFlushSize,
		FlushTime:    25,
		BlockSize:    4096,
		PrivKey:      "12345678901234567890123456789012",
	}
}

// Config used for creating the tlogserver
type Config struct {
	DataShards   int
	ParityShards int
	BlockSize    int // size of each block, used as hint for the flusher buffer size
	ListenAddr   string
	FlushSize    int
	FlushTime    int
	PrivKey      string
	AggMq        *aggmq.MQ
}

// RequiredDataServers returns how many data servers are required,
// based on the DataShards and ParityShards values.
func (conf *Config) RequiredDataServers() int {
	return conf.DataShards + conf.ParityShards
}

// flusherConfig is used by the server to create a flusher
// for a specific vdisk.
type flusherConfig struct {
	DataShards   int
	ParityShards int
	FlushSize    int
	FlushTime    int
	PrivKey      string
}
