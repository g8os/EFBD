package configV2

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"

	"syscall"

	"github.com/go-yaml/yaml"
	"github.com/zero-os/0-Disk/log"
)

// fileConfig represents a config using a YAML file as source
type fileConfig struct {
	vdiskID     string
	base        BaseConfig
	nbd         *NBDConfig
	tlog        *TlogConfig
	slave       *SlaveConfig
	baseRWLock  sync.RWMutex
	nbdRWLock   sync.RWMutex
	tlogRWLock  sync.RWMutex
	slaveRWLock sync.RWMutex
	fileRWLock  sync.RWMutex
	path        string
	done        chan interface{}
}

type yamlConfigFormat struct {
	Base  BaseConfig   `yaml:"baseConfig" valid:"required"`
	NBD   *NBDConfig   `yaml:"nbdConfig" valid:"optional"`
	Tlog  *TlogConfig  `yaml:"tlogConfig" valid:"optional"`
	Slave *SlaveConfig `yaml:"slaveConfig" valid:"optional"`
}

// NewFileConfig returns a new fileConfig from provided file path
func NewFileConfig(vdiskID string, path string) (ConfigSource, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	cfg, err := fromYAMLBytes(bytes)
	if err != nil {
		return nil, err
	}
	cfg.path = path

	// start watcher
	cfg.done = make(chan interface{})
	cfg.watch()

	return cfg, nil
}

// Close implements ConfigSource.Close
func (cfg *fileConfig) Close() error {
	// close watcher goroutine
	if cfg.done != nil {
		close(cfg.done)
	}

	return nil
}

// Base implements ConfigSource.Base
func (cfg *fileConfig) Base() BaseConfig {
	cfg.baseRWLock.RLock()
	defer cfg.baseRWLock.RUnlock()
	return cfg.base
}

// NBD implements ConfigSource.NBD
// returns ErrConfigNotAvailable when config was not found
func (cfg *fileConfig) NBD() (*NBDConfig, error) {
	cfg.nbdRWLock.RLock()
	defer cfg.nbdRWLock.RUnlock()
	if cfg.nbd == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.nbd.Clone(), nil
}

// Tlog implements ConfigSource.Tlog
// returns ErrConfigNotAvailable when config was not found
func (cfg *fileConfig) Tlog() (*TlogConfig, error) {
	cfg.tlogRWLock.RLock()
	defer cfg.tlogRWLock.RUnlock()
	if cfg.tlog == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.tlog.Clone(), nil
}

// Slave implements ConfigSource.Slave
// returns ErrConfigNotAvailable when config was not found
func (cfg *fileConfig) Slave() (*SlaveConfig, error) {
	cfg.slaveRWLock.RLock()
	defer cfg.slaveRWLock.RUnlock()
	if cfg.slave == nil {
		return nil, ErrConfigNotAvailable
	}
	return cfg.slave.Clone(), nil
}

// SetBase implements ConfigSource.SetBase
// Sets a new base config and writes it to the source
func (cfg *fileConfig) SetBase(base BaseConfig) error {
	err := base.Validate()
	if err != nil {
		return fmt.Errorf("provided base config was not valid: %s", err)
	}

	// save locally
	cfg.baseRWLock.Lock()
	cfg.base = base
	cfg.baseRWLock.Unlock()

	// write to file
	return cfg.writeToSource()
}

// SetNBD implements ConfigSource.SetNBD
// Sets a new nbd config and writes it to the source
func (cfg *fileConfig) SetNBD(nbd NBDConfig) error {
	err := nbd.Validate(cfg.base.Type)
	if err != nil {
		return fmt.Errorf("provided nbd config was not valid: %s", err)
	}

	// save locally
	cfg.nbdRWLock.Lock()
	cfg.nbd = &nbd
	cfg.nbdRWLock.Unlock()

	// write to file
	return cfg.writeToSource()
}

// SetTlog implements ConfigSource.SetTlog
// Sets a new tolog config and writes it to the source
func (cfg *fileConfig) SetTlog(tlog TlogConfig) error {
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("provided tlog config was not valid: %s", err)
	}

	// save locally
	cfg.tlogRWLock.Lock()
	cfg.tlog = &tlog
	cfg.tlogRWLock.Unlock()

	// write to file
	return cfg.writeToSource()
}

// SetSlave implements ConfigSource.SetSlave
// Sets a new slave config and writes it to the source
func (cfg *fileConfig) SetSlave(slave SlaveConfig) error {
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("provided slave config was not valid: %s", err)
	}

	// save locally
	cfg.slaveRWLock.Lock()
	cfg.slave = &slave
	cfg.slaveRWLock.Unlock()

	// write to file
	return cfg.writeToSource()
}

// watch starts watching for SIGHUP signal, then update the config
func (cfg *fileConfig) watch() {
	sighub := make(chan os.Signal)
	signal.Notify(sighub, syscall.SIGHUP)

	go func() {
		defer func() {
			signal.Stop(sighub)
			close(sighub)
		}()

		for {
			select {
			case <-sighub:
				// update config
				cfg.fileRWLock.RLock()
				bytes, err := ioutil.ReadFile(cfg.path)
				cfg.fileRWLock.RUnlock()
				if err != nil {
					log.Errorf("config was not updated, could not read from source file: %v", err)
				}
				cfgBuf, err := fromYAMLBytes(bytes)
				if err != nil {
					log.Errorf("config was not updated as read file was not valid: %v", err)
				}
				// update base
				cfg.baseRWLock.Lock()
				cfg.base = cfgBuf.base
				cfg.baseRWLock.Unlock()
				// ndb
				cfg.nbdRWLock.Lock()
				cfg.nbd = cfgBuf.nbd
				cfg.nbdRWLock.Unlock()
				//tlog
				cfg.tlogRWLock.Lock()
				cfg.tlog = cfgBuf.tlog
				cfg.tlogRWLock.Unlock()
				//slave
				cfg.slaveRWLock.Lock()
				cfg.slave = cfgBuf.slave
				cfg.slaveRWLock.Unlock()

			case <-cfg.done:
				log.Infof("Received closing signal for %s, closing watch goroutine", cfg.vdiskID)
				return
			}
		}
	}()
}

// creates config from yaml byte slice
func fromYAMLBytes(bytes []byte) (*fileConfig, error) {
	cfg := new(fileConfig)
	buf := new(yamlConfigFormat)
	// unmarshal the yaml content
	err := yaml.Unmarshal(bytes, buf)
	if err != nil {
		return nil, fmt.Errorf("Could not unmarshal provided bytes: %v", err)
	}

	// debug
	//log.Info("NBD Values: %v", buf.NBD)

	// set up buffer to be able to set private fields
	cfg.base = buf.Base
	cfg.nbd = buf.NBD
	cfg.tlog = buf.Tlog
	cfg.slave = buf.Slave

	err = cfg.base.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid base configuration: %s", err)
	}
	if cfg.nbd != nil {
		err = cfg.nbd.Validate(cfg.base.Type)
		if err != nil {
			return nil, fmt.Errorf("invalid nbd configuration: %s", err)
		}
	}
	if cfg.tlog != nil {
		err = cfg.tlog.Validate()
		if err != nil {
			return nil, fmt.Errorf("invalid tlog configuration: %s", err)
		}
	}
	if cfg.slave != nil {
		err = cfg.slave.Validate()
		if err != nil {
			return nil, fmt.Errorf("invalid slave configuration: %s", err)
		}
	}
	return cfg, nil
}

// writes the full config to the source file
func (cfg *fileConfig) writeToSource() error {
	// don't write if no path is provided
	if cfg.path == "" {
		return nil
	}

	filePerm, err := filePerm(cfg.path)
	if err != nil {
		return err
	}

	// set up buffer to be able to get private fields
	var buf yamlConfigFormat
	cfg.baseRWLock.RLock()
	buf.Base = cfg.base
	cfg.baseRWLock.RUnlock()
	cfg.nbdRWLock.RLock()
	buf.NBD = cfg.nbd
	cfg.nbdRWLock.RUnlock()
	cfg.tlogRWLock.RLock()
	buf.Tlog = cfg.tlog
	cfg.tlogRWLock.RUnlock()
	cfg.slaveRWLock.RLock()
	buf.Slave = cfg.slave
	cfg.slaveRWLock.RUnlock()

	// get bytes
	bytes, err := yaml.Marshal(buf)
	if err != nil {
		return err
	}

	// write
	cfg.fileRWLock.Lock()
	err = ioutil.WriteFile(cfg.path, bytes, filePerm)
	cfg.fileRWLock.Unlock()
	if err != nil {
		return err
	}
	// send SIGHUP
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	return nil
}

// get config file permission
// we need it because we want to rewrite it.
// better to write it with same permission
func filePerm(path string) (os.FileMode, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Mode(), nil
}
