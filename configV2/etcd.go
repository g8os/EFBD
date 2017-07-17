package configV2

import (
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
)

// etcdConfig represents a config using ETCD as source
type etcdConfig struct {
	vdiskID   string
	base      BaseConfig
	ndb       NDBConfig
	tlog      TlogConfig
	slave     SlaveConfig
	baseKey   string
	ndbKey    string
	tlogKey   string
	slaveKey  string
	endpoints []string
	cli       *clientv3.Client
	closeFunc func() error
}

// NewETCDConfig returns a new etcdConfig from provided endpoints
func NewETCDConfig(vdiskID string, endpoints []string) (ConfigSource, error) {
	cfg := new(etcdConfig)
	cfg.vdiskID = vdiskID
	cfg.endpoints = endpoints

	// setup keys
	cfg.baseKey = fmt.Sprintf("%s:%s", cfg.vdiskID, "conf")
	cfg.ndbKey = fmt.Sprintf("%s:%s:%s", cfg.vdiskID, "conf", "ndb")
	cfg.ndbKey = fmt.Sprintf("%s:%s:%s", cfg.vdiskID, "conf", "tlog")
	cfg.slaveKey = fmt.Sprintf("%s:%s:%s", cfg.vdiskID, "conf", "slave")

	// Setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	cfg.cli = cli

	// fetch all subconfigs

	return cfg, nil
}

// Close implements ConfigSource.Close
func (cfg *etcdConfig) Close() error {
	if cfg.cli == nil {
		return nil
	}
	err := cfg.cli.Close()
	if err != nil {
		return err
	}

	if cfg.closeFunc != nil {
		cfg.closeFunc()
	}

	return nil
}

// Base implements ConfigSource.Base
func (cfg *etcdConfig) Base() BaseConfig {
	return cfg.base
}

// NDB implements ConfigSource.NDB
func (cfg *etcdConfig) NDB() NDBConfig {
	return cfg.ndb
}

// Tlog implements ConfigSource.Tlog
func (cfg *etcdConfig) Tlog() TlogConfig {
	return cfg.tlog
}

// Slave implements ConfigSource.Slave
func (cfg *etcdConfig) Slave() SlaveConfig {
	return cfg.slave
}

// SetBase implements ConfigSource.SetBase
// Sets a new base config and writes it to the source
func (cfg *etcdConfig) SetBase(base BaseConfig) error {
	return nil
}

// SetNDB implements ConfigSource.SetNDB
// Sets a new ndb config and writes it to the source
func (cfg *etcdConfig) SetNDB(ndb NDBConfig) error {
	return nil
}

// SetTlog implements ConfigSource.SetTlog
// Sets a new tolog config and writes it to the source
func (cfg *etcdConfig) SetTlog(tlog TlogConfig) error {
	return nil
}

// SetSlave implements ConfigSource.SetSlave
// Sets a new slave config and writes it to the source
func (cfg *etcdConfig) SetSlave(slave SlaveConfig) error {
	return nil
}
