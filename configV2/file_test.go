package configV2

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"testing"

	"time"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

func TestFileSourceRead(t *testing.T) {
	// setup yaml file
	// write
	testfile, err := ioutil.TempFile("", "testconfig")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}

	// make sure it'll be cleaned up
	defer os.Remove(testfile.Name())

	// get BaseConfig
	base, err := ReadBaseConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	assert.Equal(t, uint64(4096), base.BlockSize)
	assert.Equal(t, false, base.ReadOnly)
	assert.Equal(t, uint64(10), base.Size)
	assert.Equal(t, VdiskTypeDB, base.Type)

	// get NBDConfig
	nbd, err := ReadNBDConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd) {
		return
	}

	assert.Equal(t, "mytemplate", nbd.TemplateVdiskID)
	assert.Equal(t, "192.168.58.146:2000", nbd.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, 0, nbd.StorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", nbd.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, 1, nbd.StorageCluster.MetadataStorage.Database)
	assert.Equal(t, "192.168.58.147:2000", nbd.TemplateStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 0, nbd.TemplateStorageCluster.DataStorage[0].Database)

	// get TlogConfig
	tlog, err := ReadTlogConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	assert.Equal(t, "192.168.58.149:2000", tlog.TlogStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, tlog.TlogStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", tlog.TlogStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, tlog.TlogStorageCluster.MetadataStorage.Database)

	// get SlaveConfig
	slave, err := ReadSlaveConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	assert.Equal(t, "192.168.58.145:2000", slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, slave.SlaveStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.144:2000", slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, slave.SlaveStorageCluster.MetadataStorage.Database)
}

func TestFileSourceWatch(t *testing.T) {
	// give the goroutines time to close
	time.Sleep(5 * time.Millisecond)
	// setup yaml file
	// write
	testfile, err := ioutil.TempFile("", "testconfigwatchersdata")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}

	// make sure it'll be cleaned up
	defer os.Remove(testfile.Name())

	// get current values
	cfg, err := readConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	// setup watchers
	ctxnbd, cancelnbd := context.WithCancel(context.Background())
	ctxtlog, canceltlog := context.WithCancel(context.Background())
	ctxslave, cancelslave := context.WithCancel(context.Background())
	defer func() {
		cancelnbd()
		canceltlog()
		cancelslave()
	}()
	var (
		nbd   NBDConfig
		tlog  TlogConfig
		slave SlaveConfig
		lock  sync.RWMutex
	)
	nbdUpdate, err := WatchNBDConfigFile(ctxnbd, testfile.Name())
	tlogUpdate, err := WatchTlogConfigFile(ctxtlog, testfile.Name())
	slaveUpdate, err := WatchSlaveConfigFile(ctxslave, testfile.Name())

	go func() {
		for {
			select {
			case <-ctxnbd.Done():
				return

			case nnbd := <-nbdUpdate:
				lock.Lock()
				nbd = nnbd
				lock.Unlock()
			case ntlog := <-tlogUpdate:
				lock.Lock()
				tlog = ntlog
				lock.Unlock()
			case nslave := <-slaveUpdate:
				lock.Lock()
				slave = nslave
				lock.Unlock()
			}
		}
	}()

	// setup new values
	newBase, err := NewBaseConfig([]byte(validBaseStr))
	newNBD, err := NewNBDConfig([]byte(validNBDStr), newBase.Type)
	newTlog, err := NewTlogConfig([]byte(validTlogStr))
	newSlave, err := NewSlaveConfig([]byte(validSlaveStr))

	// send updates
	err = writeNBDConfigFile(testfile.Name(), *newNBD, newBase.Type)
	err = writeTlogConfigFile(testfile.Name(), *newTlog)
	err = writeSlaveConfigFile(testfile.Name(), *newSlave)
	if !assert.NoError(t, err) {
		return
	}

	// send SIGHUP
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	// wait a bit
	time.Sleep(50 * time.Millisecond)

	// check if values are updated
	lock.RLock()
	// nbd
	assert.Equal(t, newNBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.Equal(t, newNBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	assert.NotEqual(t, cfg.NBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.NotEqual(t, cfg.NBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	// tlog
	assert.Equal(t, newTlog.SlaveSync, tlog.SlaveSync)
	assert.Equal(t, newTlog.TlogStorageCluster.DataStorage[0].Address, tlog.TlogStorageCluster.DataStorage[0].Address)
	assert.NotEqual(t, cfg.Tlog.SlaveSync, tlog.SlaveSync)
	assert.NotEqual(t, cfg.Tlog.TlogStorageCluster.DataStorage[0].Address, tlog.TlogStorageCluster.DataStorage[0].Address)
	// slave
	assert.Equal(t, newSlave.SlaveStorageCluster.DataStorage[0].Address, slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, newSlave.SlaveStorageCluster.MetadataStorage.Address, slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.NotEqual(t, cfg.Slave.SlaveStorageCluster.DataStorage[0].Address, slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.NotEqual(t, cfg.Slave.SlaveStorageCluster.MetadataStorage.Address, slave.SlaveStorageCluster.MetadataStorage.Address)
	lock.RUnlock()
}

// WriteNBDConfigFile writes a NBDConfig to file
func writeNBDConfigFile(path string, nbd NBDConfig, vdiskType VdiskType) error {
	// validate
	err := nbd.Validate(vdiskType)
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	cfg, _ := readConfigFile(path)

	// apply new subconfig
	cfg.NBD = &nbd

	// write new config file
	err = writeConfigFile(path, cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// WriteTlogConfigFile writes a TlogConfig to file
func writeTlogConfigFile(path string, tlog TlogConfig) error {
	// validate
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

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

// WriteSlaveConfigFile writes a SlaveConfig to file
func writeSlaveConfigFile(path string, slave SlaveConfig) error {
	// validate
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

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

// writeConfigFile writes the full config to the source file
func writeConfigFile(path string, cfg *configFileFormat) error {

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
