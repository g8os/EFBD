package configV2

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-yaml/yaml"
)

// ReadBaseConfigFile returns Baseconfig from a file
func ReadBaseConfigFile(path string) (*BaseConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return &cfg.Base, nil
}

// WriteBaseConfigFile writes a BaseConfig to file
func WriteBaseConfigFile(path string, base BaseConfig) error {
	// validate
	err := base.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	// read full file (to only change the Baseconfig, other configs would not be overriden)
	cfg, _ := readConfigFile(path)

	// apply new subconfig
	cfg.Base = base

	// write new config file
	err = writeConfigFile(path, cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// ReadNBDConfigFile returns NBDconfig from a file
func ReadNBDConfigFile(path string) (*NBDConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.NBD, nil
}

// WriteNBDConfigFile writes a NBDConfig to file
func WriteNBDConfigFile(path string, nbd NBDConfig) error {
	// read file to get base for validation
	cfg, err := readConfigFile(path)
	if err != nil {
		return fmt.Errorf("could not read file to write nbd to: %s", err)
	}

	// validate
	err = nbd.Validate(cfg.Base.Type)
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	// apply new subconfig
	cfg.NBD = &nbd

	// write new config file
	err = writeConfigFile(path, cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// ReadTlogConfigFile returns Tlogconfig from a file
func ReadTlogConfigFile(path string) (*TlogConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.Tlog, nil
}

// WriteTlogConfigFile writes a TlogConfig to file
func WriteTlogConfigFile(path string, tlog TlogConfig) error {
	// validate
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	// read full file (to only change the TlogConfig, other configs would not be overriden)
	cfg, _ := readConfigFile(path)

	// apply new subconfig
	cfg.Tlog = &tlog

	// write new config file
	err = writeConfigFile(path, cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// ReadSlaveConfigFile returns Slaveconfig from a file
func ReadSlaveConfigFile(path string) (*SlaveConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.Slave, nil
}

// WriteSlaveConfigFile writes a SlaveConfig to file
func WriteSlaveConfigFile(path string, slave SlaveConfig) error {
	// validate
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	// read full file (to only change the SlaveConfig, other configs would not be overriden)
	cfg, _ := readConfigFile(path)

	// apply new subconfig
	cfg.Slave = &slave

	// write new config file
	err = writeConfigFile(path, cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// configFile represents a config using a YAML file as source
type configFile struct {
	Base  BaseConfig   `yaml:"baseConfig" valid:"required"`
	NBD   *NBDConfig   `yaml:"nbdConfig" valid:"optional"`
	Tlog  *TlogConfig  `yaml:"tlogConfig" valid:"optional"`
	Slave *SlaveConfig `yaml:"slaveConfig" valid:"optional"`
}

// readConfigFilecreates config from yaml byte slice
// also used for testing config and etcd
func readConfigFile(path string) (*configFile, error) {
	// read file
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	return readConfigBytes(bytes)
}

func readConfigBytes(bytes []byte) (*configFile, error) {
	cfg := new(configFile)
	// unmarshal the yaml content
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return nil, fmt.Errorf("Could not unmarshal provided bytes: %v", err)
	}

	err = cfg.Base.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid base configuration: %s", err)
	}
	err = cfg.NBD.Validate(cfg.Base.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid nbd configuration: %s", err)
	}
	err = cfg.Tlog.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid tlog configuration: %s", err)
	}
	err = cfg.Slave.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid slave configuration: %s", err)
	}
	return cfg, nil
}

// writeConfigFile writes the full config to the source file
func writeConfigFile(path string, cfg *configFile) error {

	filePerm, err := filePerm(path)
	if err != nil {
		return err
	}

	// get bytes
	bytes, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	// write
	err = ioutil.WriteFile(path, bytes, filePerm)
	if err != nil {
		return err
	}

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
