package configV2

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileSource(t *testing.T) {
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

	// write a new BaseConfig
	newBase, err := NewBaseConfig([]byte(validBaseStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, newBase) {
		return
	}
	err = WriteBaseConfigFile(testfile.Name(), *newBase)
	if !assert.NoError(t, err) {
		return
	}
	// read again and check
	base, err = ReadBaseConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	assert.Equal(t, uint64(2048), base.BlockSize)
	assert.Equal(t, true, base.ReadOnly)
	assert.Equal(t, uint64(110), base.Size)
	assert.Equal(t, VdiskTypeCache, base.Type)

	// write a new NBDConfig
	newNBD, err := NewNBDConfig([]byte(validNBDStr), base.Type)
	if !assert.NoError(t, err) || !assert.NotNil(t, newNBD) {
		return
	}
	err = WriteNBDConfigFile(testfile.Name(), *newNBD)
	if !assert.NoError(t, err) {
		return
	}
	// read again and check
	nbd, err = ReadNBDConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd) {
		return
	}

	assert.Equal(t, "testtemplate", nbd.TemplateVdiskID)
	assert.Equal(t, "192.168.1.146:2000", nbd.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, 10, nbd.StorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.1.146:2001", nbd.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, 11, nbd.StorageCluster.MetadataStorage.Database)
	assert.Equal(t, "192.168.1.147:2000", nbd.TemplateStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 10, nbd.TemplateStorageCluster.DataStorage[0].Database)

	// write a new TlogConfig
	newTlog, err := NewTlogConfig([]byte(validTlogStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, newTlog) {
		return
	}
	err = WriteTlogConfigFile(testfile.Name(), *newTlog)
	if !assert.NoError(t, err) {
		return
	}
	// read again and check
	tlog, err = ReadTlogConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	assert.Equal(t, "192.168.1.1:1000", tlog.TlogStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 14, tlog.TlogStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.1.1:1001", tlog.TlogStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 18, tlog.TlogStorageCluster.MetadataStorage.Database)

	// write a new SlaveConfig
	newSlave, err := NewSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, newSlave) {
		return
	}
	err = WriteSlaveConfigFile(testfile.Name(), *newSlave)
	if !assert.NoError(t, err) {
		return
	}
	// read again and check
	slave, err = ReadSlaveConfigFile(testfile.Name())
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	assert.Equal(t, "192.168.2.149:1000", slave.SlaveStorageCluster.DataStorage[0].Address)
	assert.Equal(t, 14, slave.SlaveStorageCluster.DataStorage[0].Database)
	assert.Equal(t, "192.168.2.146:1001", slave.SlaveStorageCluster.MetadataStorage.Address)
	assert.Equal(t, 18, slave.SlaveStorageCluster.MetadataStorage.Database)
}
