package config

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-yaml/yaml"
	"github.com/zero-os/0-Disk/log"
)

// ReadBaseConfigFile returns Baseconfig from a file
func ReadBaseConfigFile(vdiskID, path string) (*BaseConfig, error) {
	// read file
	cfg, err := readVdiskConfigFile(vdiskID, path)
	if err != nil {
		return nil, err
	}

	return &cfg.Base, nil
}

// ReadNBDConfigFile returns NBDconfig from a file
func ReadNBDConfigFile(vdiskID, path string) (*BaseConfig, *NBDConfig, error) {
	// read file
	cfg, err := readVdiskConfigFile(vdiskID, path)
	if err != nil {
		return nil, nil, err
	}

	if cfg.NBD == nil {
		return nil, nil, fmt.Errorf("config file %s doesn't contain nbd config", path)
	}

	return &cfg.Base, cfg.NBD, nil
}

// WatchNBDConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchNBDConfigFile(ctx context.Context, vdiskID, path string) (<-chan NBDConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	_, nbd, err := ReadNBDConfigFile(vdiskID, path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial NBDconfig for NBdConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan NBDConfig, 1)
	updater <- *nbd

	go watchConfigFile(ctx, vdiskID, path, func(cfg *vdiskConfigFileFormat) {
		if cfg.NBD == nil {
			log.Errorf("no nbd cfg in file %s, while nbd watcher requires it", path)
			return
		}

		// send current data to channel
		select {
		case updater <- *cfg.NBD:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}
	})

	return updater, nil
}

// ReadTlogConfigFile returns Tlogconfig from a file
func ReadTlogConfigFile(vdiskID, path string) (*TlogConfig, error) {
	// read file
	cfg, err := readVdiskConfigFile(vdiskID, path)
	if err != nil {
		return nil, err
	}

	if cfg.Tlog == nil {
		return nil, fmt.Errorf("config file %s doesn't contain tlog config", path)
	}

	return cfg.Tlog, nil
}

// WatchTlogConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchTlogConfigFile(ctx context.Context, vdiskID, path string) (<-chan TlogConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	tlog, err := ReadTlogConfigFile(vdiskID, path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial TlogConfig for TlogConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan TlogConfig, 1)
	updater <- *tlog

	go watchConfigFile(ctx, vdiskID, path, func(cfg *vdiskConfigFileFormat) {
		if cfg.Tlog == nil {
			log.Errorf("no tlog cfg in file %s, while tlog watcher requires it", path)
			return
		}

		// send current data to channel
		select {
		case updater <- *cfg.Tlog:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}
	})

	return updater, nil
}

// ReadSlaveConfigFile returns Slaveconfig from a file
func ReadSlaveConfigFile(vdiskID, path string) (*SlaveConfig, error) {
	// read file
	cfg, err := readVdiskConfigFile(vdiskID, path)
	if err != nil {
		return nil, err
	}

	if cfg.Slave == nil {
		return nil, fmt.Errorf("config file %s doesn't contain slave config", path)
	}

	return cfg.Slave, nil
}

// WatchSlaveConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchSlaveConfigFile(ctx context.Context, vdiskID, path string) (<-chan SlaveConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	slave, err := ReadSlaveConfigFile(vdiskID, path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial SlaveConfig for SlaveConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan SlaveConfig, 1)
	updater <- *slave

	go watchConfigFile(ctx, vdiskID, path, func(cfg *vdiskConfigFileFormat) {
		if cfg.Slave == nil {
			log.Errorf("no slave cfg in file %s, while slave watcher requires it", path)
			return
		}

		// send current data to channel
		select {
		case updater <- *cfg.Slave:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}
	})

	return updater, nil
}

// ReadVdisksConfigFile returns a requested VdisksConfig from a file
func ReadVdisksConfigFile(path string) (*VdisksConfig, error) {
	// read file
	cfg, err := readFullConfigFile(path)
	if err != nil {
		return nil, err
	}

	vdisksConfig := new(VdisksConfig)
	for vdiskID := range *cfg {
		vdisksConfig.List = append(vdisksConfig.List, vdiskID)
	}

	return vdisksConfig, nil
}

// configFile represents a config using a YAML file as source
type configFileFormat map[string]vdiskConfigFileFormat

// vdiskConfigFileFormat represents a vdisk's config
// as found in a YAML file
type vdiskConfigFileFormat struct {
	Base  BaseConfig   `yaml:"base" valid:"required"`
	NBD   *NBDConfig   `yaml:"nbd" valid:"optional"`
	Tlog  *TlogConfig  `yaml:"tlog" valid:"optional"`
	Slave *SlaveConfig `yaml:"slave" valid:"optional"`
}

// readVdiskConfigFile creates a vdisk config from yaml byte slice
// also used for testing config and etcd
func readVdiskConfigFile(vdiskID string, path string) (*vdiskConfigFileFormat, error) {
	// read file
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	return readVdiskConfigBytes(vdiskID, bytes)
}

// readFullConfigFile creates a full config from yaml byte slice
// also used for testing config and etcd
func readFullConfigFile(path string) (*configFileFormat, error) {
	// read file
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	return readFullConfigBytes(bytes)
}

func readFullConfigBytes(bytes []byte) (*configFileFormat, error) {
	fileCfg := new(configFileFormat)

	// unmarshal the yaml content
	err := yaml.Unmarshal(bytes, fileCfg)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal provided bytes: %v", err)
	}

	if fileCfg == nil || len(*fileCfg) == 0 {
		return nil, errors.New("no vdisk configs available")
	}

	return fileCfg, nil
}

func readVdiskConfigBytes(vdiskID string, bytes []byte) (*vdiskConfigFileFormat, error) {
	fileCfg, err := readFullConfigBytes(bytes)
	if err != nil {
		return nil, err
	}

	cfg, ok := (*fileCfg)[vdiskID]
	if !ok {
		return nil, fmt.Errorf(
			"vdisk %s wasn't specified in the given YAML config", vdiskID)
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

	return &cfg, nil
}

// watchConfigFile watches for SIGHUP and updates subconfig
func watchConfigFile(ctx context.Context, vdiskID, path string, useConfig func(*vdiskConfigFileFormat)) {
	// setup SIGHUP
	sighup := make(chan os.Signal)
	signal.Notify(sighup, syscall.SIGHUP)
	defer signal.Stop(sighup)
	defer close(sighup)

	log.Debug("Started watch goroutine for SIGHUP")
	defer log.Debugf("Closing SIGHUP watch goroutine for %s", path)

	for {
		select {
		case <-ctx.Done():
			return
		case <-sighup:
			log.Debug("Received SIGHUP for: ", path)
			// read config file
			cfg, err := readVdiskConfigFile(vdiskID, path)
			if err != nil {
				log.Errorf("Could not get config from file: %s", err)
				continue
			}

			// send config to handler
			useConfig(cfg)
		}
	}
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
