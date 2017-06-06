package config

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

type testReloader struct {
	vdiskID string
	done    chan struct{}
	ch      chan VdiskClusterConfig
	cfg     VdiskClusterConfig
	hr      HotReloader
	t       *testing.T
	mux     sync.Mutex
}

func newTestReloader(t *testing.T, hr HotReloader, vdiskID string) *testReloader {
	tr := &testReloader{
		vdiskID: vdiskID,
		done:    make(chan struct{}),
		ch:      make(chan VdiskClusterConfig),
		t:       t,
		hr:      hr,
	}

	if err := hr.Subscribe(tr.ch, tr.vdiskID); err != nil {
		tr.t.Fatal(err)
	}

	cfg, err := hr.VdiskClusterConfig(vdiskID)
	if err != nil {
		tr.t.Fatal(err)
	}
	tr.cfg = *cfg

	go tr.listen()

	return tr
}

func (tr *testReloader) TestReceivedConfig(address string) {
	tr.mux.Lock()
	defer tr.mux.Unlock()

	if assert.Len(tr.t, tr.cfg.DataCluster.DataStorage, 1) {
		assert.Equal(tr.t, address, tr.cfg.DataCluster.DataStorage[0].Address)
		if assert.NotNil(tr.t, tr.cfg.DataCluster.MetadataStorage) {
			assert.Equal(tr.t, address, tr.cfg.DataCluster.MetadataStorage.Address)
			return
		}
	}

	tr.t.Fatal("TestReceivedConfig failed")
}

func (tr *testReloader) Close() {
	tr.t.Log("closing testReloader")
	close(tr.done)
}

func (tr *testReloader) listen() {
	defer func() {
		err := tr.hr.Unsubscribe(tr.ch)
		if err != nil {
			if err != nil {
				tr.t.Fatal(err)
			}
		}
	}()

	for {
		select {
		case cfg := <-tr.ch:
			func() {
				tr.mux.Lock()
				defer tr.mux.Unlock()
				tr.cfg = cfg
			}()
		case <-tr.done:
			return
		}
	}
}

func wait(t *testing.T) {
	t.Log("sleeping 300ms...")
	time.Sleep(300 * time.Millisecond)
}

func sendSIGHUP(t *testing.T, msg string) {
	t.Log("sighup -> ", msg)
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
}

func TestHotReloader(t *testing.T) {
	configPath := createTempFile(t)
	writeTestConfig(t, configPath, "localhost:16379", "a", "b")

	hr, err := NewHotReloader(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !assert.Subset(t, hr.VdiskIdentifiers(), []string{"a", "b"}) {
		return
	}

	go hr.Listen(context.Background())
	defer func() {
		if err := hr.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	wait(t)

	sendSIGHUP(t, "ensure signal never hangs (even if no reloaders are registered)")

	wait(t)

	t.Log("register 2 reloaders")

	reloaderA := newTestReloader(t, hr, "a")
	reloaderB := newTestReloader(t, hr, "b")

	t.Log("ensure configs have been set upon registration")
	reloaderA.TestReceivedConfig("localhost:16379")
	reloaderB.TestReceivedConfig("localhost:16379")

	writeTestConfig(t, configPath, "localhost:16380", "a", "b")
	if !assert.Subset(t, hr.VdiskIdentifiers(), []string{"a", "b"}) {
		return
	}

	sendSIGHUP(t, "ensure configs are updated")

	wait(t)

	reloaderA.TestReceivedConfig("localhost:16380")
	reloaderB.TestReceivedConfig("localhost:16380")

	t.Log("unregister 1 reloader, and update both configs again")

	reloaderA.Close()

	writeTestConfig(t, configPath, "localhost:16381", "a", "b")
	if !assert.Subset(t, hr.VdiskIdentifiers(), []string{"a", "b"}) {
		return
	}

	sendSIGHUP(t, "ensure config is updated")

	wait(t)

	reloaderA.TestReceivedConfig("localhost:16380")
	reloaderB.TestReceivedConfig("localhost:16381")

	t.Log("unregister last reloader")

	reloaderB.Close()

	writeTestConfig(t, configPath, "localhost:16382", "a", "b")
	if !assert.Subset(t, hr.VdiskIdentifiers(), []string{"a", "b"}) {
		return
	}

	sendSIGHUP(t, "ensure signal never hangs (even if no reloaders are registered)")

	wait(t)

	reloaderA.TestReceivedConfig("localhost:16380")
	reloaderB.TestReceivedConfig("localhost:16381")
}

func createTempFile(t *testing.T) string {
	file, err := ioutil.TempFile("", "testReloader")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	return file.Name()
}

func writeTestConfig(t *testing.T, path, scaddr string, vdiskids ...string) {
	config := &Config{
		Vdisks: make(map[string]VdiskConfig),
		StorageClusters: map[string]StorageClusterConfig{
			"mycluster": StorageClusterConfig{
				DataStorage: []StorageServerConfig{
					StorageServerConfig{Address: scaddr},
				},
				MetadataStorage: &StorageServerConfig{Address: scaddr},
			},
		},
	}

	for _, vdiskID := range vdiskids {
		config.Vdisks[vdiskID] = VdiskConfig{
			BlockSize:      4096,
			ReadOnly:       false,
			Size:           10,
			StorageCluster: "mycluster",
			Type:           VdiskTypeBoot,
		}
	}

	os.Remove(path)

	err := ioutil.WriteFile(path, []byte(config.String()), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	err = ValidateConfigPath(path)
	if err != nil {
		t.Fatal(err)
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
