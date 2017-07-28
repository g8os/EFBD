package config

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
	"github.com/zero-os/0-Disk/log"
)

const (
	vdiskID = "testVdisk"
)

func TestFileSourceRead(t *testing.T) {
	// setup yaml file
	// write
	testfile, err := ioutil.TempFile("", "testconfig")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testfile.Name())

	base, err := ReadBaseConfigFile(vdiskID, testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	assert.Equal(t, uint64(4096), base.BlockSize)
	assert.Equal(t, false, base.ReadOnly)
	assert.Equal(t, uint64(10), base.Size)
	assert.Equal(t, VdiskTypeDB, base.Type)

	// get NBDConfig
	base, nbd, err := ReadNBDConfigFile(vdiskID, testfile.Name())
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
	tlog, err := ReadTlogConfigFile(vdiskID, testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	assert.Equal(t, "192.168.58.149:2000", tlog.TlogStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, tlog.TlogStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.146:2001", tlog.TlogStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, tlog.TlogStorageCluster.MetadataStorage.Database)

	// get SlaveConfig
	slave, err := ReadSlaveConfigFile(vdiskID, testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	assert.Equal(t, "192.168.58.145:2000", slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 4, slave.SlaveStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.58.144:2000", slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 8, slave.SlaveStorageCluster.MetadataStorage.Database)
}

func TestFileSourceInvalidRead(t *testing.T) {
	// check if error is returned when reading non existing file
	// setup temp dir with non existing yamlfile path
	tempdir, err := ioutil.TempDir("", "doesnotcontainyamlsource")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(tempdir)
	nullPath := tempdir + "/doesntexist.yaml"

	_, baseerr := ReadBaseConfigFile(vdiskID, nullPath)
	_, _, nbderr := ReadNBDConfigFile(vdiskID, nullPath)
	_, tlogerr := ReadTlogConfigFile(vdiskID, nullPath)
	_, slaveerr := ReadSlaveConfigFile(vdiskID, nullPath)

	// check if error is returned when reading non empty file
	log.Debugf("Errors reading from non existing file:\n base error: %s \n nbd error: %s \n tlog error: %s \n slave error: %s", baseerr, nbderr, tlogerr, slaveerr)
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
	testfile, err := ioutil.TempFile("", "testonlybasedata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testfile.Name())

	writeConfigFile(testfile.Name(), &cfg)

	// read
	_, baseerr = ReadBaseConfigFile(vdiskID, testfile.Name())
	_, _, nbderr = ReadNBDConfigFile(vdiskID, testfile.Name())
	_, tlogerr = ReadTlogConfigFile(vdiskID, testfile.Name())
	_, slaveerr = ReadSlaveConfigFile(vdiskID, testfile.Name())

	// check if error is returned when reading non empty file
	// base should be fine tho
	log.Debugf("Errors reading from base only file:\n base error: %s \n nbd error: %s \n tlog error: %s \n slave error: %s", baseerr, nbderr, tlogerr, slaveerr)
	assert.NoError(t, baseerr)
	assert.Error(t, nbderr)
	assert.Error(t, tlogerr)
	assert.Error(t, slaveerr)

	// write only nbd config
	// missing base should make it fail
	nobasefile, err := ioutil.TempFile("", "testnobasedata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(nobasefile.Name())
	vcfg.Base = *new(BaseConfig)
	vcfg.NBD, _ = NewNBDConfig([]byte(validNBDStr), b.Type)
	cfg[vdiskID] = vcfg
	writeConfigFile(nobasefile.Name(), &cfg)

	_, _, nbderr = ReadNBDConfigFile(vdiskID, nobasefile.Name())
	log.Debugf("Error from reading NBD with missing BaseConfig: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid nbd config
	invalnbdfile, err := ioutil.TempFile("", "testinvalnbddata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(invalnbdfile.Name())
	invalnbdfile.Write([]byte(invalidNBDServerConfigs[4]))

	_, _, nbderr = ReadNBDConfigFile(vdiskID, invalnbdfile.Name())
	log.Debugf("Error from reading invalid NBD: %s", nbderr)
	assert.Error(t, nbderr)

	// write invalid tlog config
	invaltlogfile, err := ioutil.TempFile("", "testinvaltlogdata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(invaltlogfile.Name())
	invaltlogfile.Write([]byte(invalidNBDServerConfigs[7]))

	_, tlogerr = ReadTlogConfigFile(vdiskID, invaltlogfile.Name())
	log.Debugf("Error from reading invalid Tlog: %s", tlogerr)
	assert.Error(t, tlogerr)

	// write invalid slave config
	invalslavefile, err := ioutil.TempFile("", "testinvalslavedata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(invalslavefile.Name())
	invalslavefile.Write([]byte(invalidNBDServerConfigs[8]))

	_, slaveerr = ReadSlaveConfigFile(vdiskID, invalslavefile.Name())
	log.Debugf("Error from reading invalid Slave: %s", slaveerr)
	assert.Error(t, slaveerr)
}

func TestFileSourceWatch(t *testing.T) {
	// give the goroutines time to close
	time.Sleep(10 * time.Millisecond)
	// setup yaml file
	// write
	testfile, err := ioutil.TempFile("", "testconfigwatchersdata")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testfile.Name())

	// get current values
	cfg, err := readVdiskConfigFile(vdiskID, testfile.Name())
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
	nbdUpdate, err := WatchNBDConfigFile(ctxnbd, vdiskID, testfile.Name())
	tlogUpdate, err := WatchTlogConfigFile(ctxtlog, vdiskID, testfile.Name())
	slaveUpdate, err := WatchSlaveConfigFile(ctxslave, vdiskID, testfile.Name())

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
	err = writeNBDConfigFile(vdiskID, testfile.Name(), newNBD, newBase.Type)
	err = writeTlogConfigFile(vdiskID, testfile.Name(), newTlog)
	err = writeSlaveConfigFile(vdiskID, testfile.Name(), newSlave)
	if !assert.NoError(t, err) {
		return
	}

	// send SIGHUP
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	// wait a bit
	time.Sleep(100 * time.Millisecond)

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
	// give the goroutines time to close
	time.Sleep(10 * time.Millisecond)

	// setup temp dir with non existing yamlfile path
	tempdir, err := ioutil.TempDir("", "doesnotcontainyamlsource")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(tempdir)
	nullPath := tempdir + "/doesntexist.yaml"

	// try and read, it should fail
	ctxNullnbd, cancelNullnbd := context.WithCancel(context.Background())
	ctxNulltlog, cancelNulltlog := context.WithCancel(context.Background())
	ctxNullslave, cancelNullslave := context.WithCancel(context.Background())
	_, errNullnbd := WatchNBDConfigFile(ctxNullnbd, vdiskID, nullPath)
	_, errNulltlog := WatchTlogConfigFile(ctxNulltlog, vdiskID, nullPath)
	_, errNullslave := WatchSlaveConfigFile(ctxNullslave, vdiskID, nullPath)
	cancelNullnbd()
	cancelNulltlog()
	cancelNullslave()

	log.Debugf("Errors from non existing file:\n nbd error: %s \n tlog error: %s \n slave error: %s", errNullnbd, errNulltlog, errNullslave)
	if !assert.Error(t, errNullnbd) || !assert.Error(t, errNulltlog) || !assert.Error(t, errNullslave) {
		return
	}

	// setup empty yaml file
	emptyfile, err := ioutil.TempFile("", "testemptyconfigwatchersdata")
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(emptyfile.Name())

	// try and read the file, it should fail
	ctxEmptynbd, cancelEmptynbd := context.WithCancel(context.Background())
	ctxEmptytlog, cancelEmptytlog := context.WithCancel(context.Background())
	ctxEmptyslave, cancelEmptyslave := context.WithCancel(context.Background())
	_, errnbd := WatchNBDConfigFile(ctxEmptynbd, vdiskID, emptyfile.Name())
	_, errtlog := WatchTlogConfigFile(ctxEmptytlog, vdiskID, emptyfile.Name())
	_, errslave := WatchSlaveConfigFile(ctxEmptyslave, vdiskID, emptyfile.Name())
	cancelEmptynbd()
	cancelEmptytlog()
	cancelEmptyslave()

	log.Debugf("Errors from empty file:\n nbd error: %s \n tlog error: %s \n slave error: %s", errnbd, errtlog, errslave)
	if !assert.Error(t, errnbd) || !assert.Error(t, errtlog) || !assert.Error(t, errslave) {
		return
	}

	// setup yaml file with invalid configs
	// write
	testfile, err := ioutil.TempFile("", "testinvalidconfigwatchersdata")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testfile.Name())

	// get current values
	cfg, err := readVdiskConfigFile(vdiskID, testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

}

func TestMissingConfigFileSourceWhileWatch(t *testing.T) {
	// setup yaml file with missing configs
	testmissfile, err := ioutil.TempFile("", "testmissingconfigwatchersdata")
	_, err = testmissfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testmissfile.Name())

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
	nbdUpdate, errnbd := WatchNBDConfigFile(ctxnbd, vdiskID, testmissfile.Name())
	tlogUpdate, errtlog := WatchTlogConfigFile(ctxtlog, vdiskID, testmissfile.Name())
	slaveUpdate, errslave := WatchSlaveConfigFile(ctxslave, vdiskID, testmissfile.Name())
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
	writeNBDConfigFile(vdiskID, testmissfile.Name(), nil, VdiskTypeDB)
	writeTlogConfigFile(vdiskID, testmissfile.Name(), nil)
	writeSlaveConfigFile(vdiskID, testmissfile.Name(), nil)

	// send sighup
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)

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
	testfile, err := ioutil.TempFile("", "testconfigwatchersdata")
	_, err = testfile.Write([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(testfile.Name())
	_, errnbd := WatchNBDConfigFile(nil, vdiskID, testfile.Name())
	_, errtlog := WatchTlogConfigFile(nil, vdiskID, testfile.Name())
	_, errslave := WatchSlaveConfigFile(nil, vdiskID, testfile.Name())

	assert.NoError(t, errnbd)
	assert.NoError(t, errtlog)
	assert.NoError(t, errslave)
}

// WriteNBDConfigFile writes a NBDConfig to file
func writeNBDConfigFile(vdiskID, path string, nbd *NBDConfig, vdiskType VdiskType) error {
	// validate
	err := nbd.Validate(vdiskType)
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	cfg, _ := readFullConfig(path)

	// apply new subconfig
	vdiskCfg := cfg[vdiskID]
	vdiskCfg.NBD = nbd
	cfg[vdiskID] = vdiskCfg

	// write new config file
	err = writeConfigFile(path, &cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// WriteTlogConfigFile writes a TlogConfig to file
func writeTlogConfigFile(vdiskID, path string, tlog *TlogConfig) error {
	// validate
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	cfg, _ := readFullConfig(path)

	// apply new subconfig
	vdiskCfg := cfg[vdiskID]
	vdiskCfg.Tlog = tlog
	cfg[vdiskID] = vdiskCfg

	// write new config file
	err = writeConfigFile(path, &cfg)
	if err != nil {
		return fmt.Errorf("could not write config file: %s", err)
	}

	return nil
}

// WriteSlaveConfigFile writes a SlaveConfig to file
func writeSlaveConfigFile(vdiskID, path string, slave *SlaveConfig) error {
	// validate
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("config to be written was not valid: %s", err)
	}

	cfg, _ := readFullConfig(path)

	// apply new subconfig
	vdiskCfg := cfg[vdiskID]
	vdiskCfg.Slave = slave
	cfg[vdiskID] = vdiskCfg

	// write new config file
	err = writeConfigFile(path, &cfg)
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

func readFullConfig(path string) (configFileFormat, error) {
	// read file
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read from the config file: %s", err.Error())
	}

	cfgFile := make(configFileFormat)

	// unmarshal the yaml content
	err = yaml.Unmarshal(bytes, &cfgFile)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal provided bytes: %v", err)
	}

	return cfgFile, nil

}
