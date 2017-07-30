package config

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

const (
	vdiskID = "testVdisk"
)

// acts as the file for testing
var testFiles = make(map[string][]byte)

func TestFileSourceRead(t *testing.T) {
	// for testing, the path can be left empty, the data will be read from testFile
	testFiles["TestFileSourceRead"] = []byte(validYAMLSourceStr)

	// get base
	base, err := ReadBaseConfigFile(vdiskID, "TestFileSourceRead")
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	assert.Equal(t, uint64(4096), base.BlockSize)
	assert.Equal(t, false, base.ReadOnly)
	assert.Equal(t, uint64(10), base.Size)
	assert.Equal(t, VdiskTypeDB, base.Type)

	// get NBDConfig
	base, nbd, err := ReadNBDConfigFile(vdiskID, "TestFileSourceRead")
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
	tlog, err := ReadTlogConfigFile(vdiskID, "TestFileSourceRead")
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	assert.Equal(t, "192.168.58.149:2000", tlog.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, tlog.StorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", tlog.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, tlog.StorageCluster.MetadataStorage.Database)

	// get SlaveConfig
	slave, err := ReadSlaveConfigFile(vdiskID, "TestFileSourceRead")
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	assert.Equal(t, "192.168.58.145:2000", slave.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, slave.StorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.144:2000", slave.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, slave.StorageCluster.MetadataStorage.Database)
}

func TestFileSourceInvalidRead(t *testing.T) {
	testFiles["TestFileSourceInvalidRead"] = []byte("")

	// check if error is returned when reading empty file
	_, baseerr := ReadBaseConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, _, nbderr := ReadNBDConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, tlogerr := ReadTlogConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, slaveerr := ReadSlaveConfigFile(vdiskID, "TestFileSourceInvalidRead")

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
	testFiles["TestFileSourceInvalidRead"], _ = cfg.Bytes()

	// read
	_, baseerr = ReadBaseConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, _, nbderr = ReadNBDConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, tlogerr = ReadTlogConfigFile(vdiskID, "TestFileSourceInvalidRead")
	_, slaveerr = ReadSlaveConfigFile(vdiskID, "TestFileSourceInvalidRead")

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
	testFiles["TestFileSourceInvalidRead"], _ = cfg.Bytes()

	_, _, nbderr = ReadNBDConfigFile(vdiskID, "TestFileSourceInvalidRead")
	log.Debugf("Error from reading NBD with missing BaseConfig: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid nbd config
	testFiles["TestFileSourceInvalidRead"] = []byte(invalidNBDServerConfigs[4])
	_, _, nbderr = ReadNBDConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid NBD: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid tlog config
	testFiles["TestFileSourceInvalidRead"] = []byte(invalidNBDServerConfigs[7])
	_, tlogerr = ReadTlogConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid Tlog: %s", tlogerr)
	assert.Error(t, tlogerr)

	// write invalid slave config
	testFiles["TestFileSourceInvalidRead"] = []byte(invalidNBDServerConfigs[8])
	_, slaveerr = ReadSlaveConfigFile(vdiskID, "")
	log.Debugf("Error from reading invalid Slave: %s", slaveerr)
	assert.Error(t, slaveerr)
}

func TestFileSourceWatch(t *testing.T) {
	assert := assert.New(t)

	// get start values
	testFiles["TestFileSourceWatch"] = []byte(validYAMLSourceStr)
	cfg, err := readVdiskConfigFile(vdiskID, "TestFileSourceWatch")
	if !assert.NoError(err) || !assert.NotNil(cfg) {
		return
	}

	// setup watchers
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var (
		nbd   NBDConfig
		tlog  TlogConfig
		slave SlaveConfig
		lock  sync.Mutex
	)

	nbdUpdate, err := WatchNBDConfigFile(ctx, vdiskID, "TestFileSourceWatch")
	if !assert.NoError(err) || !assert.NotNil(nbdUpdate) {
		return
	}
	nbd = <-nbdUpdate // get initial config

	tlogUpdate, err := WatchTlogConfigFile(ctx, vdiskID, "TestFileSourceWatch")
	if !assert.NoError(err) || !assert.NotNil(tlogUpdate) {
		return
	}
	tlog = <-tlogUpdate // get initial config

	slaveUpdate, err := WatchSlaveConfigFile(ctx, vdiskID, "TestFileSourceWatch")
	if !assert.NoError(err) || !assert.NotNil(slaveUpdate) {
		return
	}
	slave = <-slaveUpdate // get initial config

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case nnbd := <-nbdUpdate:
				lock.Lock()
				nbd = nnbd
				wg.Done()
				lock.Unlock()

			case ntlog := <-tlogUpdate:
				lock.Lock()
				tlog = ntlog
				wg.Done()
				lock.Unlock()
			case nslave := <-slaveUpdate:
				lock.Lock()
				slave = nslave
				wg.Done()
				lock.Unlock()
			}
		}
	}()

	// setup new values
	cfgBuf := make(configFileFormat)
	var vcfg vdiskConfigFileFormat

	b, err := NewBaseConfig([]byte(validBaseStr)) // just to have a valid base config
	if !assert.NoError(err) {
		return
	}
	newNBD, err := NewNBDConfig([]byte(validNBDStr), VdiskTypeCache)
	if !assert.NoError(err) {
		return
	}
	newTlog, err := NewTlogConfig([]byte(validTlogStr))
	if !assert.NoError(err) {
		return
	}
	newSlave, err := NewSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(err) {
		return
	}

	vcfg.Base = *b
	vcfg.NBD = newNBD
	vcfg.Tlog = newTlog
	vcfg.Slave = newSlave
	cfgBuf[vdiskID] = vcfg
	testFiles["TestFileSourceWatch"], err = cfgBuf.Bytes()
	if !assert.NoError(err) {
		return
	}

	// send SIGHUP
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	wg.Wait()

	// check if values are updated
	lock.Lock()
	defer lock.Unlock()

	// nbd
	assert.Equal(newNBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.Equal(newNBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	assert.NotEqual(cfg.NBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.NotEqual(cfg.NBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	// tlog
	assert.Equal(newTlog.SlaveSync, tlog.SlaveSync)
	assert.Equal(newTlog.StorageCluster.DataStorage[0].Address, tlog.StorageCluster.DataStorage[0].Address)
	assert.NotEqual(cfg.Tlog.SlaveSync, tlog.SlaveSync)
	assert.NotEqual(cfg.Tlog.StorageCluster.DataStorage[0].Address, tlog.StorageCluster.DataStorage[0].Address)
	// slave
	assert.Equal(newSlave.StorageCluster.DataStorage[0].Address, slave.StorageCluster.DataStorage[0].Address)
	assert.Equal(newSlave.StorageCluster.MetadataStorage.Address, slave.StorageCluster.MetadataStorage.Address)
	assert.NotEqual(cfg.Slave.StorageCluster.DataStorage[0].Address, slave.StorageCluster.DataStorage[0].Address)
	assert.NotEqual(cfg.Slave.StorageCluster.MetadataStorage.Address, slave.StorageCluster.MetadataStorage.Address)
}

func TestInvalidFileSourceWatch(t *testing.T) {
	assert := assert.New(t)

	// setup empty yaml file
	testFiles["TestInvalidFileSourceWatch"] = []byte("")

	// try and read the file, it should fail
	ctxEmpty, cancelEmpty := context.WithCancel(context.Background())
	_, errnbd := WatchNBDConfigFile(ctxEmpty, vdiskID, "TestInvalidFileSourceWatch")
	_, errtlog := WatchTlogConfigFile(ctxEmpty, vdiskID, "TestInvalidFileSourceWatch")
	_, errslave := WatchSlaveConfigFile(ctxEmpty, vdiskID, "TestInvalidFileSourceWatch")
	cancelEmpty()

	log.Debugf("Errors from empty file:\n nbd error: %s \n tlog error: %s \n slave error: %s", errnbd, errtlog, errslave)
	if !assert.Error(errnbd) || !assert.Error(errtlog) || !assert.Error(errslave) {
		return
	}

	// setup yaml file with valid configs later replace with invalid
	// write
	testFiles["TestInvalidFileSourceWatch"] = []byte(validYAMLSourceStr)

	// get current values
	cfg, err := readVdiskConfigFile(vdiskID, "TestInvalidFileSourceWatch")
	if !assert.NoError(err) || !assert.NotNil(cfg) {
		return
	}

	// setup watchers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		nbd   NBDConfig
		tlog  TlogConfig
		slave SlaveConfig
	)
	nbdUpdate, err := WatchNBDConfigFile(ctx, vdiskID, "TestInvalidFileSourceWatch")
	if !assert.NoError(err) {
		return
	}
	nbd = <-nbdUpdate // initial config
	tlogUpdate, err := WatchTlogConfigFile(ctx, vdiskID, "TestInvalidFileSourceWatch")
	if !assert.NoError(err) {
		return
	}
	tlog = <-tlogUpdate // initial config
	slaveUpdate, err := WatchSlaveConfigFile(ctx, vdiskID, "TestInvalidFileSourceWatch")
	if !assert.NoError(err) {
		return
	}
	slave = <-slaveUpdate // initial config

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case nnbd := <-nbdUpdate:
				assert.FailNow("should not happen: %v", nnbd)
			case ntlog := <-tlogUpdate:
				assert.FailNow("should not happen: %v", ntlog)
			case nslave := <-slaveUpdate:
				assert.FailNow("should not happen: %v", nslave)
			}
		}
	}()

	// update with invalid nbd data
	testFiles["TestInvalidFileSourceWatch"] = []byte(invalidNBDServerConfigs[5])
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	time.Sleep(1 * time.Second)

	// check if old values are persisted
	assert.Equal(cfg.NBD.StorageCluster.DataStorage[0].Address, nbd.StorageCluster.DataStorage[0].Address)
	assert.Equal(cfg.Tlog.StorageCluster.DataStorage[0].Database, tlog.StorageCluster.DataStorage[0].Database)
	assert.Equal(cfg.Slave.StorageCluster.DataStorage[0].Address, slave.StorageCluster.DataStorage[0].Address)
}

func TestMissingConfigFileSourceWhileWatch(t *testing.T) {
	assert := assert.New(t)

	// setup yaml file later, make data go missing
	testFiles["TestMissingConfigFileSourceWhileWatch"] = []byte(validYAMLSourceStr)

	var (
		nbd   NBDConfig
		tlog  TlogConfig
		slave SlaveConfig
	)

	// setup watchers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nbdUpdate, err := WatchNBDConfigFile(ctx, vdiskID, "TestMissingConfigFileSourceWhileWatch")
	if !assert.NoError(err) {
		return
	}
	nbd = <-nbdUpdate // initial config
	tlogUpdate, err := WatchTlogConfigFile(ctx, vdiskID, "TestMissingConfigFileSourceWhileWatch")
	if !assert.NoError(err) {
		return
	}
	tlog = <-tlogUpdate // initial config
	slaveUpdate, err := WatchSlaveConfigFile(ctx, vdiskID, "TestMissingConfigFileSourceWhileWatch")
	if !assert.NoError(err) {
		return
	}
	slave = <-slaveUpdate // initial config

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case nnbd := <-nbdUpdate:
				assert.FailNow("should not happen: %v", nnbd)
			case ntlog := <-tlogUpdate:
				assert.FailNow("should not happen: %v", ntlog)
			case nslave := <-slaveUpdate:
				assert.FailNow("should not happen: %v", nslave)
			}
		}
	}()

	// remove configs
	testFiles["TestMissingConfigFileSourceWhileWatch"] = []byte("")

	// send sighup
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	time.Sleep(1 * time.Second)

	// check if still original values
	cfg, err := readVdiskConfigBytes(vdiskID, []byte(validYAMLSourceStr))
	if !assert.NoError(err) {
		return
	}

	// nbd
	assert.NotEmpty(nbd.TemplateVdiskID)
	assert.Equal(cfg.NBD.TemplateVdiskID, nbd.TemplateVdiskID)
	assert.Equal(cfg.NBD.StorageCluster.MetadataStorage.Address, nbd.StorageCluster.MetadataStorage.Address)
	// tlog
	assert.NotEmpty(tlog.StorageCluster)
	assert.Equal(cfg.Tlog.SlaveSync, tlog.SlaveSync)
	assert.Equal(cfg.Tlog.StorageCluster.MetadataStorage.Address, tlog.StorageCluster.MetadataStorage.Address)
	assert.NotEmpty(tlog.StorageCluster)
	// slave
	assert.Equal(cfg.Slave.StorageCluster.DataStorage[0].Address, slave.StorageCluster.DataStorage[0].Address)
	assert.Equal(cfg.Slave.StorageCluster.MetadataStorage.Address, slave.StorageCluster.MetadataStorage.Address)
	assert.NotEmpty(slave.StorageCluster)

}

func TestFileSourceWatchNilCtx(t *testing.T) {
	testFiles["TestFileSourceWatchNilCtx"] = []byte(validYAMLSourceStr)
	_, errnbd := WatchNBDConfigFile(nil, vdiskID, "TestFileSourceWatchNilCtx")
	_, errtlog := WatchTlogConfigFile(nil, vdiskID, "TestFileSourceWatchNilCtx")
	_, errslave := WatchSlaveConfigFile(nil, vdiskID, "TestFileSourceWatchNilCtx")

	assert.NoError(t, errnbd)
	assert.NoError(t, errtlog)
	assert.NoError(t, errslave)
}

func init() {
	// stub ioutil
	readFile = func(path string) ([]byte, error) {
		if content, ok := testFiles[path]; ok {
			return content, nil
		}

		return nil, fmt.Errorf("no content available for %s", path)
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
