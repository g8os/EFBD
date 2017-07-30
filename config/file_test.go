package config

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

const (
	vdiskID = "testVdisk"
)

// acts as the file for testing
var testFile []byte

func TestFileSourceRead(t *testing.T) {
	// for testing, the path can be left empty, the data will be read from testFile
	testFile = []byte(validYAMLSourceStr)

	// get base
	base, err := ReadBaseConfigFile(vdiskID, "")
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	assert.Equal(t, uint64(4096), base.BlockSize)
	assert.Equal(t, false, base.ReadOnly)
	assert.Equal(t, uint64(10), base.Size)
	assert.Equal(t, VdiskTypeDB, base.Type)

	// get NBDConfig
	base, nbd, err := ReadNBDConfigFile(vdiskID, "")
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd) {
		return
	}

	assert.Equal(t, uint64(4096), base.BlockSize)
	assert.Equal(t, false, base.ReadOnly)
	assert.Equal(t, uint64(10), base.Size)
	assert.Equal(t, VdiskTypeDB, base.Type)

	assert.Equal(t, "mytemplate", nbd.TemplateVdiskID)
	assert.Equal(t, "192.168.58.146:2000", nbd.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, 0, nbd.StorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", nbd.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, 1, nbd.StorageCluster.MetadataStorage.Database)
	assert.Equal(t, "192.168.58.147:2000", nbd.TemplateStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 0, nbd.TemplateStorageCluster.DataStorage[0].Database)

	// get TlogConfig
	tlog, err := ReadTlogConfigFile(vdiskID, "")
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	assert.Equal(t, "192.168.58.149:2000", tlog.TlogStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, tlog.TlogStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", tlog.TlogStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, tlog.TlogStorageCluster.MetadataStorage.Database)

	// get SlaveConfig
	slave, err := ReadSlaveConfigFile(vdiskID, "")
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	assert.Equal(t, "192.168.58.145:2000", slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, slave.SlaveStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.144:2000", slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, slave.SlaveStorageCluster.MetadataStorage.Database)
}

func TestFileSourceInvalidRead(t *testing.T) {
	testFile = []byte("")

	// check if error is returned when reading empty file
	_, baseerr := ReadBaseConfigFile(vdiskID, "")
	_, _, nbderr := ReadNBDConfigFile(vdiskID, "")
	_, tlogerr := ReadTlogConfigFile(vdiskID, "")
	_, slaveerr := ReadSlaveConfigFile(vdiskID, "")

	// check if error is returned when reading non empty file
	log.Debugf("Errors reading from empty file:\n base error: %s \n nbd error: %s \n tlog error: %s \n slave error: %s", baseerr, nbderr, tlogerr, slaveerr)
	assert.Error(t, baseerr)
	assert.Error(t, nbderr)
	assert.Error(t, tlogerr)
	assert.Error(t, slaveerr)

	// write file with only base config
	// read other subconfig and see if they return an error
	cfg := make(configFileFormat)
	var vcfg vdiskConfigFileFormat
	b, _ := NewBaseConfig([]byte(validBaseStr))
	vcfg.Base = *b
	cfg[vdiskID] = vcfg
	testFile, _ = cfg.Bytes()

	// read
	_, baseerr = ReadBaseConfigFile(vdiskID, "")
	_, _, nbderr = ReadNBDConfigFile(vdiskID, "")
	_, tlogerr = ReadTlogConfigFile(vdiskID, "")
	_, slaveerr = ReadSlaveConfigFile(vdiskID, "")

	// check if error is returned when reading non empty file
	// base should be fine tho
	log.Debugf("Errors reading from base only file:\n base error: %s \n nbd error: %s \n tlog error: %s \n slave error: %s", baseerr, nbderr, tlogerr, slaveerr)
	assert.NoError(t, baseerr)
	assert.Error(t, nbderr)
	assert.Error(t, tlogerr)
	assert.Error(t, slaveerr)

	// write only nbd config
	// missing base should make it fail
	vcfg = *new(vdiskConfigFileFormat)
	vcfg.NBD, _ = NewNBDConfig([]byte(validNBDStr), b.Type)
	cfg[vdiskID] = vcfg
	testFile, _ = cfg.Bytes()

	_, _, nbderr = ReadNBDConfigFile(vdiskID, "")
	log.Debugf("Error from reading NBD with missing BaseConfig: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid nbd config
	testFile = []byte(invalidNBDServerConfigs[4])
	_, _, nbderr = ReadNBDConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid NBD: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid tlog config
	testFile = []byte(invalidNBDServerConfigs[7])
	_, tlogerr = ReadTlogConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid Tlog: %s", tlogerr)
	assert.Error(t, tlogerr)

	// write invalid slave config
	testFile = []byte(invalidNBDServerConfigs[8])
	_, slaveerr = ReadSlaveConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid Slave: %s", slaveerr)
	assert.Error(t, slaveerr)
}

func TestFileSourceWatch(t *testing.T) {
	// get start values
	testFile = []byte(validYAMLSourceStr)
	cfg, err := readVdiskConfigFile(vdiskID, "")
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
	nbdUpdate, err := WatchNBDConfigFile(ctxnbd, vdiskID, "")
	tlogUpdate, err := WatchTlogConfigFile(ctxtlog, vdiskID, "")
	slaveUpdate, err := WatchSlaveConfigFile(ctxslave, vdiskID, "")

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
	cfgBuf := make(configFileFormat)
	var vcfg vdiskConfigFileFormat
	b, _ := NewBaseConfig([]byte(validBaseStr)) // just to have a valid base config
	newNBD, _ := NewNBDConfig([]byte(validNBDStr), VdiskTypeCache)
	newTlog, _ := NewTlogConfig([]byte(validTlogStr))
	newSlave, _ := NewSlaveConfig([]byte(validSlaveStr))
	vcfg.Base = *b
	cfgBuf[vdiskID] = vcfg
	println(cfgBuf)
	testFile, _ = cfgBuf.Bytes()

	// send SIGHUP
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

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

func TestInvalidFileSourceWatch(t *testing.T) {

	// setup empty yaml file
	testFile = []byte("")

	// try and read the file, it should fail
	ctxEmptynbd, cancelEmptynbd := context.WithCancel(context.Background())
	ctxEmptytlog, cancelEmptytlog := context.WithCancel(context.Background())
	ctxEmptyslave, cancelEmptyslave := context.WithCancel(context.Background())
	_, errnbd := WatchNBDConfigFile(ctxEmptynbd, vdiskID, "")
	_, errtlog := WatchTlogConfigFile(ctxEmptytlog, vdiskID, "")
	_, errslave := WatchSlaveConfigFile(ctxEmptyslave, vdiskID, "")
	cancelEmptynbd()
	cancelEmptytlog()
	cancelEmptyslave()

	log.Debugf("Errors from empty file:\n nbd error: %s \n tlog error: %s \n slave error: %s", errnbd, errtlog, errslave)
	if !assert.Error(t, errnbd) || !assert.Error(t, errtlog) || !assert.Error(t, errslave) {
		return
	}

	// setup yaml file with valid configs later replace with invalid
	// write
	testFile = []byte(validYAMLSourceStr)

	// get current values
	cfg, err := readVdiskConfigFile(vdiskID, "")
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
	nbdUpdate, err := WatchNBDConfigFile(ctxnbd, vdiskID, "")
	tlogUpdate, err := WatchTlogConfigFile(ctxtlog, vdiskID, "")
	slaveUpdate, err := WatchSlaveConfigFile(ctxslave, vdiskID, "")

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

	// update with invalid nbd data
	testFile = []byte(invalidNBDServerConfigs[5])
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	// check if old values are persisted
	assert.Equal(t, cfg.NBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, cfg.NBD.TemplateStorageCluster.DataStorage[0].Database, nbd.TemplateStorageCluster.DataStorage[0].Database)

}

func TestMissingConfigFileSourceWhileWatch(t *testing.T) {
	// setup yaml file later, make data go missing
	testFile = []byte(validYAMLSourceStr)

	var (
		nbd   NBDConfig
		tlog  TlogConfig
		slave SlaveConfig
		lock  sync.RWMutex
	)

	// setup watchers
	ctxnbd, cancelnbd := context.WithCancel(context.Background())
	ctxtlog, canceltlog := context.WithCancel(context.Background())
	ctxslave, cancelslave := context.WithCancel(context.Background())
	nbdUpdate, errnbd := WatchNBDConfigFile(ctxnbd, vdiskID, "")
	tlogUpdate, errtlog := WatchTlogConfigFile(ctxtlog, vdiskID, "")
	slaveUpdate, errslave := WatchSlaveConfigFile(ctxslave, vdiskID, "")
	defer func() {
		cancelnbd()
		canceltlog()
		cancelslave()
	}()
	if !assert.NoError(t, errnbd) || !assert.NoError(t, errtlog) || !assert.NoError(t, errslave) {
		return
	}

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

	// remove configs
	testFile = []byte("")

	// send sighup
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	// check if still original values
	cfg, err := readVdiskConfigBytes(vdiskID, []byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	lock.RLock()
	// nbd
	assert.NotEmpty(t, nbd.TemplateVdiskID)
	assert.Equal(t, cfg.NBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.Equal(t, cfg.NBD.StorageCluster.MetadataStorage.Address, nbd.StorageCluster.MetadataStorage.Address)
	// tlog
	assert.NotEmpty(t, tlog.TlogStorageCluster)
	assert.Equal(t, cfg.Tlog.SlaveSync, tlog.SlaveSync)
	assert.Equal(t, cfg.Tlog.TlogStorageCluster.MetadataStorage.Address, tlog.TlogStorageCluster.MetadataStorage.Address)
	assert.NotEmpty(t, tlog.TlogStorageCluster)
	// slave
	assert.Equal(t, cfg.Slave.SlaveStorageCluster.DataStorage[0].Address, slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, cfg.Slave.SlaveStorageCluster.MetadataStorage.Address, slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.NotEmpty(t, slave.SlaveStorageCluster)
	lock.RUnlock()

}

func TestFileSourceWatchNilCtx(t *testing.T) {
	testFile = []byte(validYAMLSourceStr)
	_, errnbd := WatchNBDConfigFile(nil, vdiskID, "")
	_, errtlog := WatchTlogConfigFile(nil, vdiskID, "")
	_, errslave := WatchSlaveConfigFile(nil, vdiskID, "")

	assert.NoError(t, errnbd)
	assert.NoError(t, errtlog)
	assert.NoError(t, errslave)
}

func init() {
	// stub ioutil
	readFile = func(path string) ([]byte, error) {
		return testFile, nil
	}
}
