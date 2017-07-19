package configV2

import (
	"io/ioutil"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"os"

	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

func TestFileSource(t *testing.T) {
	cfg, err := fromYAMLBytes([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}
	defer func() {
		err = cfg.Close()
		assert.NoError(t, err)
	}()

	base := BaseConfig{
		BlockSize: 1234,
		ReadOnly:  true,
		Size:      2,
		Type:      VdiskTypeBoot,
	}

	nbd := cfg.nbd
	nbd.TemplateVdiskID = "test"

	tlog := cfg.tlog
	tlog.TlogStorageCluster.DataStorage[0].Address = "1.2.3.4:1234"

	slave, err := newSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(t, err) {
		return
	}

	// test if new base is set
	err = cfg.SetBase(base)
	if !assert.Equal(t, base.BlockSize, cfg.base.BlockSize) {
		return
	}
	if !assert.Equal(t, base.ReadOnly, cfg.base.ReadOnly) {
		return
	}
	if !assert.Equal(t, base.Size, cfg.base.Size) {
		return
	}
	if !assert.Equal(t, base.Type, cfg.base.Type) {
		return
	}

	// test if new nbd is set
	cfg.SetNBD(*nbd)
	if !assert.Equal(t, nbd.TemplateVdiskID, cfg.nbd.TemplateVdiskID) {
		return
	}

	// test if new tlog is set
	cfg.SetTlog(*tlog)
	if !assert.Equal(t, tlog.TlogStorageCluster.DataStorage[0].Address, cfg.tlog.TlogStorageCluster.DataStorage[0].Address) {
		return
	}

	// test if new slave is set
	cfg.SetSlave(*slave)
	if !assert.Equal(t, slave.SlaveStorageCluster.DataStorage[0].Address, cfg.slave.SlaveStorageCluster.DataStorage[0].Address) {
		return
	}
}

func testFileSourceWithWatcher(t *testing.T) {
	// setup yaml file
	// write
	testpath := "./test.yaml"
	fileperm, err := filePerm(testpath)
	if err != nil {
		fileperm = 0644
	}
	err = ioutil.WriteFile(testpath, []byte(validYAMLSourceStr), fileperm)
	if !assert.NoError(t, err) {
		return
	}
	// make sure it'll be cleaned up
	defer os.Remove(testpath)

	// setup config
	vdiskID := "testConfig"
	cfg, err := NewFileConfig(vdiskID, testpath)
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	// set a subconfig and listen for SIGHUP
	// setup sighup listener
	sighup := sighupListener()
	defer close(sighup)

	// create new tlog
	tlog, err := newTlogConfig([]byte(validTlogStr))
	if !assert.NoError(t, err) {
		return
	}
	oldTlog, _ := cfg.Tlog()
	// set new tlog, triggering SIGHUP
	err = cfg.SetTlog(*tlog)
	if !assert.NoError(t, err) {
		return
	}

	// wait for SIGHUP with timeout
	var recievedLock sync.Mutex
	received := false
	timeout := make(chan bool, 1)
	defer close(timeout)
	go func() {
		time.Sleep(100 * time.Millisecond)
		recievedLock.Lock()
		if !received {
			timeout <- true
		}
		recievedLock.Unlock()
	}()
	select {
	case <-sighup:
		recievedLock.Lock()
		received = true
		recievedLock.Unlock()
	case <-timeout:
		recievedLock.Lock()
		received = false
		recievedLock.Unlock()
	}
	recievedLock.Lock()
	defer recievedLock.Unlock()
	if !assert.True(t, received) {
		log.Error("Did not receive SIGHUP before timeout")
		return
	}

	// compare some values
	assert.NotEqual(t, tlog.SlaveSync, oldTlog.SlaveSync)
	assert.NotEqual(t, tlog.TlogStorageCluster.DataStorage[0].Address, oldTlog.TlogStorageCluster.DataStorage[0].Address)
}

func sighupListener() chan bool {
	received := make(chan bool)
	go func() {
		sighub := make(chan os.Signal)
		signal.Notify(sighub, syscall.SIGHUP)
		defer func() {
			signal.Stop(sighub)
			close(sighub)
		}()

		for {
			select {
			case <-sighub:
				received <- true
			case _, ok := <-received:
				if !ok {
					log.Infof("closing listener goroutine")
					return
				}
			}
		}
	}()

	return received
}
