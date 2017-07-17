package configV2

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-yaml/yaml"
)

// yamlConfig represents a config using a YAML file as source
type yamlConfig struct {
	vdiskID   string
	base      BaseConfig
	ndb       NDBConfig
	tlog      TlogConfig
	slave     SlaveConfig
	path      string
	closeFunc func() error
}

type configWrapper struct {
	Base  BaseConfig  `yaml:"baseConfig" valid:"required"`
	NDB   NDBConfig   `yaml:"ndbConfig" valid:"required"`
	Tlog  TlogConfig  `yaml:"tlogConfig" valid:"required"`
	Slave SlaveConfig `yaml:"slaveConfig" valid:"optional"`
}

// NewYAMLConfig returns a new yamlConfig from provided file path
func NewYAMLConfig(vdiskID string, path string) (ConfigSource, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	cfg, err := fromYAMLBytes(bytes)
	if err != nil {
		return nil, err
	}
	cfg.path = path

	return cfg, nil
}

// Close implements ConfigSource.Close
func (cfg *yamlConfig) Close() error {
	// if custom close function is set, use it, else default
	if cfg.closeFunc != nil {
		return cfg.closeFunc()
	}
	return nil
}

// Base implements ConfigSource.Base
func (cfg *yamlConfig) Base() BaseConfig {
	return cfg.base
}

// NDB implements ConfigSource.NDB
func (cfg *yamlConfig) NDB() NDBConfig {
	return cfg.ndb
}

// Tlog implements ConfigSource.Tlog
func (cfg *yamlConfig) Tlog() TlogConfig {
	return cfg.tlog
}

// Slave implements ConfigSource.Slave
func (cfg *yamlConfig) Slave() SlaveConfig {
	return cfg.slave
}

// SetBase implements ConfigSource.SetBase
// Sets a new base config and writes it to the source
func (cfg *yamlConfig) SetBase(base BaseConfig) error {
	err := base.validate()
	if err != nil {
		return fmt.Errorf("Provided base config was not valid: %s", err)
	}
	cfg.base = base

	// write to file
	return cfg.writeToSource()
}

// SetNDB implements ConfigSource.SetNDB
// Sets a new ndb config and writes it to the source
func (cfg *yamlConfig) SetNDB(ndb NDBConfig) error {
	err := ndb.validate(cfg.vdiskID, cfg.base.Type)
	if err != nil {
		return fmt.Errorf("Provided ndb config was not valid: %s", err)
	}
	cfg.ndb = ndb

	// write to file
	return cfg.writeToSource()
}

// SetTlog implements ConfigSource.SetTlog
// Sets a new tolog config and writes it to the source
func (cfg *yamlConfig) SetTlog(tlog TlogConfig) error {
	err := tlog.validate()
	if err != nil {
		return fmt.Errorf("Provided tlog config was not valid: %s", err)
	}
	cfg.tlog = tlog

	// write to file
	return cfg.writeToSource()
}

// SetSlave implements ConfigSource.SetSlave
// Sets a new slave config and writes it to the source
func (cfg *yamlConfig) SetSlave(slave SlaveConfig) error {
	err := slave.validate()
	if err != nil {
		return fmt.Errorf("Provided slave config was not valid: %s", err)
	}
	cfg.slave = slave

	// write to file
	return cfg.writeToSource()
}

// creates config from yaml byte slice
func fromYAMLBytes(bytes []byte) (*yamlConfig, error) {
	cfg := new(yamlConfig)
	buf := new(configWrapper)
	// unmarshal the yaml content
	err := yaml.Unmarshal(bytes, buf)
	if err != nil {
		return cfg, err
	}

	// set up buffer to be able to set private fields
	cfg.base = buf.Base
	cfg.ndb = buf.NDB
	cfg.tlog = buf.Tlog
	cfg.slave = buf.Slave

	err = cfg.base.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid base configuration: %s", err)
	}
	err = cfg.ndb.validate(cfg.vdiskID, cfg.base.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid ndb configuration: %s", err)
	}
	err = cfg.tlog.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid tlog configuration: %s", err)
	}
	err = cfg.slave.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid slave configuration: %s", err)
	}

	return cfg, nil
}

// writes the full config to the source file
func (cfg *yamlConfig) writeToSource() error {
	// don't write if no path is provided
	if cfg.path == "" {
		return nil
	}

	// get config file permission
	// we need it because we want to rewrite it.
	// better to write it with same permission
	filePerm, err := func() (os.FileMode, error) {
		info, err := os.Stat(cfg.path)
		if err != nil {
			return 0, err
		}
		return info.Mode(), nil
	}()
	if err != nil {
		return err
	}

	// set up buffer to be able to get private fields
	var buf configWrapper
	buf.Base = cfg.base
	buf.NDB = cfg.ndb
	buf.Tlog = cfg.tlog
	buf.Slave = cfg.slave

	// get bytes
	bytes, err := yaml.Marshal(buf)
	if err != nil {
		return err
	}

	// write
	return ioutil.WriteFile(cfg.path, bytes, filePerm)
}
