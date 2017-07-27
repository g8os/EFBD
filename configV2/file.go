package configV2

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-yaml/yaml"
	"github.com/zero-os/0-Disk/log"
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

// ReadNBDConfigFile returns NBDconfig from a file
func ReadNBDConfigFile(path string) (*NBDConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.NBD, nil
}

// WatchNBDConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchNBDConfigFile(ctx context.Context, path string) (<-chan NBDConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	nbd, err := ReadNBDConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial NBDconfig for NBdConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan NBDConfig, 1)
	updater <- *nbd

	go watchConfigFile(ctx, path, func(cfg *configFileFormat) {
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
func ReadTlogConfigFile(path string) (*TlogConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.Tlog, nil
}

// WatchTlogConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchTlogConfigFile(ctx context.Context, path string) (<-chan TlogConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	nbd, err := ReadTlogConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial TlogConfig for TlogConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan TlogConfig, 1)
	updater <- *nbd

	go watchConfigFile(ctx, path, func(cfg *configFileFormat) {
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
func ReadSlaveConfigFile(path string) (*SlaveConfig, error) {
	// read file
	cfg, err := readConfigFile(path)
	if err != nil {
		return nil, err
	}

	return cfg.Slave, nil
}

// WatchSlaveConfigFile listens to SIGHUP for updates
// sends the current config to the channel when created
func WatchSlaveConfigFile(ctx context.Context, path string) (<-chan SlaveConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	nbd, err := ReadSlaveConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial SlaveConfig for SlaveConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan SlaveConfig, 1)
	updater <- *nbd

	go watchConfigFile(ctx, path, func(cfg *configFileFormat) {
		// send current data to channel
		select {
		case updater <- *cfg.Slave:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}
	})

	return updater, nil
}

// configFile represents a config using a YAML file as source
type configFileFormat struct {
	Base  BaseConfig   `yaml:"baseConfig" valid:"required"`
	NBD   *NBDConfig   `yaml:"nbdConfig" valid:"optional"`
	Tlog  *TlogConfig  `yaml:"tlogConfig" valid:"optional"`
	Slave *SlaveConfig `yaml:"slaveConfig" valid:"optional"`
}

// readConfigFilecreates config from yaml byte slice
// also used for testing config and etcd
func readConfigFile(path string) (*configFileFormat, error) {
	// read file
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	return readConfigBytes(bytes)
}

func readConfigBytes(bytes []byte) (*configFileFormat, error) {
	cfg := new(configFileFormat)
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

// watchConfigFile watches for SIGHUP and updates subconfig
func watchConfigFile(ctx context.Context, path string, useConfig func(*configFileFormat)) {
	// setup SIGHUP
	sighup := make(chan os.Signal)
	signal.Notify(sighup, syscall.SIGHUP)
	defer signal.Stop(sighup)
	defer close(sighup)

	log.Debug("watch goroutine for SIGHUP started")
	defer log.Debug("watch goroutine for SIGHUP closed")

	for {
		select {
		case <-ctx.Done():
			return
		case <-sighup:
			log.Debug("SIGHUP received")
			// read config file
			cfg, err := readConfigFile(path)
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
