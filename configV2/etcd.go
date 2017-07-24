package configV2

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/log"
)

// etcdConfig represents a config using etcd as source
type etcdConfig struct {
	vdiskID     string
	base        BaseConfig
	nbd         *NBDConfig
	tlog        *TlogConfig
	slave       *SlaveConfig
	baseKey     string
	nbdKey      string
	tlogKey     string
	slaveKey    string
	baseRWLock  sync.RWMutex
	nbdRWLock   sync.RWMutex
	tlogRWLock  sync.RWMutex
	slaveRWLock sync.RWMutex
	endpoints   []string
	cli         *clientv3.Client
}

// NewETCDConfig returns a new etcdConfig from provided endpoints
func NewETCDConfig(vdiskID string, endpoints []string) (ConfigSource, error) {
	cfg := new(etcdConfig)
	cfg.vdiskID = vdiskID
	cfg.endpoints = endpoints

	// setup keys
	cfg.baseKey = etcdBaseKey(vdiskID)
	cfg.nbdKey = etcdNBDKey(vdiskID)
	cfg.tlogKey = etcdTlogKey(vdiskID)
	cfg.slaveKey = etcdSlaveKey(vdiskID)

	// Setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	cfg.cli = cli

	// fetch all subconfigs
	//base
	resp, err := cli.Get(context.TODO(), cfg.baseKey)
	if err != nil {
		return nil, fmt.Errorf("could not get base from ETCD: %v", err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("base config was not found on the ETCD server")
	}
	base, err := NewBaseConfig(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	cfg.baseRWLock.Lock()
	cfg.base = *base
	cfg.baseRWLock.Unlock()
	//nbd
	resp, err = cli.Get(context.TODO(), cfg.nbdKey)
	if err != nil {
		return nil, fmt.Errorf("could not get nbd from ETCD: %v", err)
	}
	if len(resp.Kvs) > 0 {
		cfg.nbdRWLock.Lock()
		cfg.baseRWLock.RLock()
		cfg.nbd, err = NewNBDConfig(resp.Kvs[0].Value, cfg.base.Type)
		cfg.baseRWLock.RUnlock()
		cfg.nbdRWLock.Unlock()
		if err != nil {
			return nil, err
		}
	}
	//tlog
	resp, err = cli.Get(context.TODO(), cfg.tlogKey)
	if err != nil {
		return nil, fmt.Errorf("could not get tlog from ETCD: %v", err)
	}
	if len(resp.Kvs) > 0 {
		cfg.tlogRWLock.Lock()
		cfg.tlog, err = NewTlogConfig(resp.Kvs[0].Value)
		cfg.tlogRWLock.Unlock()
		if err != nil {
			return nil, err
		}
	}
	//slave
	resp, err = cli.Get(context.TODO(), cfg.slaveKey)
	if err != nil {
		return nil, fmt.Errorf("could not get slave from ETCD: %v", err)
	}
	if len(resp.Kvs) > 0 {
		cfg.slaveRWLock.Lock()
		cfg.slave, err = NewSlaveConfig(resp.Kvs[0].Value)
		cfg.slaveRWLock.Unlock()
		if err != nil {
			return nil, err
		}
	}

	// start watcher
	cfg.watch()

	return cfg, nil
}

// Close implements ConfigSource.Close
func (cfg *etcdConfig) Close() error {
	// close client
	return cfg.cli.Close()
}

// Base implements ConfigSource.Base
func (cfg *etcdConfig) Base() BaseConfig {
	cfg.baseRWLock.RLock()
	defer cfg.baseRWLock.RUnlock()
	return cfg.base
}

// NBD implements ConfigSource.NBD
// returns ErrConfigNotAvailable when config was not found
func (cfg *etcdConfig) NBD() (*NBDConfig, error) {
	cfg.nbdRWLock.RLock()
	defer cfg.nbdRWLock.RUnlock()
	if cfg.nbd == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.nbd.Clone(), nil
}

// Tlog implements ConfigSource.Tlog
// returns ErrConfigNotAvailable when config was not found
func (cfg *etcdConfig) Tlog() (*TlogConfig, error) {
	cfg.tlogRWLock.RLock()
	defer cfg.tlogRWLock.RUnlock()
	if cfg.tlog == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.tlog.Clone(), nil
}

// Slave implements ConfigSource.Slave
// returns ErrConfigNotAvailable when config was not found
func (cfg *etcdConfig) Slave() (*SlaveConfig, error) {
	cfg.slaveRWLock.RLock()
	defer cfg.slaveRWLock.RUnlock()
	if cfg.slave == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.slave.Clone(), nil
}

// SetBase implements ConfigSource.SetBase
// Sets a new base config and writes it to the source
func (cfg *etcdConfig) SetBase(base BaseConfig) error {
	err := base.Validate()
	if err != nil {
		return fmt.Errorf("provided base config was not valid: %s", err)
	}

	// write to etcd
	val, err := base.ToBytes()
	if err != nil {
		return err
	}
	_, err = cfg.cli.Put(context.TODO(), cfg.baseKey, string(val))
	if err != nil {
		return fmt.Errorf("could not send base config: %v", err)
	}

	return nil
}

// SetNBD implements ConfigSource.SetNBD
// Sets a new nbd config and writes it to the source
func (cfg *etcdConfig) SetNBD(nbd NBDConfig) error {
	cfg.baseRWLock.RLock()
	err := nbd.Validate(cfg.base.Type)
	cfg.baseRWLock.RUnlock()
	if err != nil {
		return fmt.Errorf("provided nbd config was not valid: %s", err)
	}

	// save locally
	cfg.nbdRWLock.Lock()
	cfg.nbd = &nbd
	cfg.nbdRWLock.Unlock()

	// write to etcd
	val, err := nbd.ToBytes()
	if err != nil {
		return err
	}
	_, err = cfg.cli.Put(context.TODO(), cfg.nbdKey, string(val))
	if err != nil {
		return fmt.Errorf("could not send nbd config: %v", err)
	}

	return nil
}

// SetTlog implements ConfigSource.SetTlog
// Sets a new tolog config and writes it to the source
func (cfg *etcdConfig) SetTlog(tlog TlogConfig) error {
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("provided tlog config was not valid: %s", err)
	}

	// save locally
	cfg.nbdRWLock.Lock()
	cfg.tlog = &tlog
	cfg.nbdRWLock.Unlock()

	// write to etcd
	val, err := tlog.ToBytes()
	if err != nil {
		return err
	}
	_, err = cfg.cli.Put(context.TODO(), cfg.tlogKey, string(val))
	if err != nil {
		return fmt.Errorf("could not send tlog config: %v", err)
	}

	return nil
}

// SetSlave implements ConfigSource.SetSlave
// Sets a new slave config and writes it to the source
func (cfg *etcdConfig) SetSlave(slave SlaveConfig) error {
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("provided slave config was not valid: %s", err)
	}

	// save locally
	cfg.slaveRWLock.Lock()
	cfg.slave = &slave
	cfg.slaveRWLock.Unlock()

	// write to etcd
	val, err := slave.ToBytes()
	if err != nil {
		return err
	}
	_, err = cfg.cli.Put(context.TODO(), cfg.slaveKey, string(val))
	if err != nil {
		return fmt.Errorf("could not send slave config: %v", err)
	}

	return nil
}

// Set a watcher for a field
func (cfg *etcdConfig) watch() {
	// watch for fields starting with the config's vdiskID
	watchkey := cfg.vdiskID + ":conf:"
	watch := cfg.cli.Watch(context.TODO(), watchkey, clientv3.WithPrefix())
	go func() {
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel encountered an error: %v", resp.Err())
				}
				log.Infof("Watch channel for %s closed", cfg.vdiskID)
				return
			}
			// update value of key
			for _, ev := range resp.Events {
				log.Infof("Value for %s received an update", ev.Kv.Key)
				// switch on key returned from watch without the watchkey prefix
				switch string(strings.TrimPrefix(string(ev.Kv.Key), watchkey)) {
				case "base":
					// check if empty
					if len(ev.Kv.Value) < 1 {
						log.Errorf("base config for %s was empty on ETCD, keeping the old one", cfg.vdiskID)
						continue
					}

					newBase, err := NewBaseConfig(ev.Kv.Value)
					if err != nil {
						log.Errorf("Did not update %s: %s", cfg.baseKey, err)
					}
					cfg.baseRWLock.Lock()
					cfg.base = *newBase
					cfg.baseRWLock.Unlock()
				case "nbd":
					// check if empty, if so make the subconfig nil
					if len(ev.Kv.Value) < 1 {
						cfg.nbdRWLock.Lock()
						cfg.nbd = nil
						cfg.nbdRWLock.Unlock()
						log.Infof("%s was found empty on ETCD, setting to nbd nil", ev.Kv.Key)
						continue
					}

					// create subconfig and check validity
					cfg.baseRWLock.RLock()
					newNBD, err := NewNBDConfig(ev.Kv.Value, cfg.base.Type)
					if err != nil {
						log.Errorf("Did not update %s: %s", cfg.nbdKey, err)
						cfg.baseRWLock.RUnlock()
						continue
					}
					cfg.baseRWLock.RUnlock()
					cfg.nbdRWLock.Lock()
					cfg.nbd = newNBD
					cfg.nbdRWLock.Unlock()
				case "tlog":
					if len(ev.Kv.Value) < 1 {
						cfg.tlogRWLock.Lock()
						cfg.tlog = nil
						cfg.tlogRWLock.Unlock()
						log.Infof("%s was found empty on ETCD, setting to tlog nil", ev.Kv.Key)
						continue
					}

					newTlog, err := NewTlogConfig(ev.Kv.Value)
					if err != nil {
						log.Errorf("Did not update %s: %s", cfg.tlogKey, err)
						continue
					}
					cfg.tlogRWLock.Lock()
					cfg.tlog = newTlog
					cfg.tlogRWLock.Unlock()
				case "slave":
					if len(ev.Kv.Value) < 1 {
						cfg.slaveRWLock.Lock()
						cfg.slave = nil
						cfg.slaveRWLock.Unlock()
						log.Infof("%s was found empty on ETCD, setting to slave nil", ev.Kv.Key)
						continue
					}

					newSlave, err := NewSlaveConfig(ev.Kv.Value)
					if err != nil {
						log.Errorf("Did not update %s: %s", cfg.slaveKey, err)
						continue
					}
					cfg.slaveRWLock.Lock()
					cfg.slave = newSlave
					cfg.slaveRWLock.Unlock()
				}
			}
		}
	}()
}

// etcdBaseKey returns base key for provided vdiskid
// update the watcher if keys are changed
func etcdBaseKey(vdiskID string) string {
	return vdiskID + ":conf:base"
}

// etcdNBDKey returns nbd key for provided vdiskid
func etcdNBDKey(vdiskID string) string {
	return vdiskID + ":conf:nbd"
}

// etcdTlogKey returns tlog key for provided vdiskid
func etcdTlogKey(vdiskID string) string {
	return vdiskID + ":conf:tlog"
}

// etcdSlaveKey returns slave key for provided vdiskid
func etcdSlaveKey(vdiskID string) string {
	return vdiskID + ":conf:slave"
}
