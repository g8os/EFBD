package configV2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

// test only works if local etcd is running
func testETCDConfig(t *testing.T) {
	//init
	log.SetLevel(log.DebugLevel)
	endpoints := []string{"127.0.0.1:2379"}
	vdiskID := "test"
	baseKey := etcdBaseKey(vdiskID)
	nbdKey := etcdNBDKey(vdiskID)
	tlogKey := etcdTlogKey(vdiskID)
	slaveKey := etcdSlaveKey(vdiskID)

	// input data
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if !assert.NoError(t, err) || !assert.NotNil(t, cli) {
		return
	}
	defer cli.Close()
	// cleanup ETCD before and after use
	cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)
	defer cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)

	ops := []clientv3.Op{
		clientv3.OpPut(baseKey, validBaseStr),
		clientv3.OpPut(nbdKey, validNBDStr),
		clientv3.OpPut(tlogKey, validTlogStr),
		clientv3.OpPut(slaveKey, validSlaveStr),
	}

	for _, op := range ops {
		_, err := cli.Do(context.TODO(), op)
		if !assert.NoError(t, err) {
			return
		}
	}

	base, err := ReadBaseConfigETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	ba, _ := NewBaseConfig([]byte(validBaseStr))
	if !assert.Equal(t, ba.BlockSize, base.BlockSize) {
		return
	}

	nbd, err := ReadNBDConfigETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd) {
		return
	}

	tlog, err := ReadTlogConfigETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	slave, err := ReadSlaveConfigETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	// NBD watch
	ctx, cancel := context.WithCancel(context.Background())
	nbdUpdate, err := WatchNBDConfigETCD(ctx, vdiskID, endpoints)
	go func() {
		for {
			nbd, ok := <-nbdUpdate
			if !ok {
				log.Debug("nbd update listener closed")
				return
			}
			log.Debugf("nbd update listener reciever: %v", nbd)
		}
	}()

	// send updates
	err = writeNBDConfigETCD(vdiskID, endpoints, *nbd)
	if !assert.NoError(t, err) {
		cancel()
		return
	}
	log.Debug("Sending empty value to nbd")
	writeConfigETCD(endpoints, nbdKey, []byte{})
	// allow time to listen for the updates
	time.Sleep(1 * time.Millisecond)
	cancel()

	// Tlog watch
	ctx, cancel = context.WithCancel(context.Background())
	tlogUpdate, err := WatchTlogConfigETCD(ctx, vdiskID, endpoints)
	go func() {
		for {
			tlog, ok := <-tlogUpdate
			if !ok {
				log.Debug("tlog update listener closed")
				return
			}
			log.Debugf("tlog update listener reciever: %v", tlog)
		}
	}()
	// send updates
	err = writeTlogConfigETCD(vdiskID, endpoints, *tlog)
	if !assert.NoError(t, err) {
		cancel()
		return
	}
	log.Debug("Sending empty value to tlog")
	writeConfigETCD(endpoints, tlogKey, []byte{})
	// allow time to listen for the updates
	time.Sleep(1 * time.Millisecond)
	cancel()

	// Slave watch
	ctx, cancel = context.WithCancel(context.Background())
	slaveUpdate, err := WatchSlaveConfigETCD(ctx, vdiskID, endpoints)
	go func() {
		for {
			slave, ok := <-slaveUpdate
			if !ok {
				log.Debug("slave update listener closed")
				return
			}
			log.Debugf("slave update listener reciever: %v", slave)
		}
	}()

	// send updates
	err = writeSlaveConfigETCD(vdiskID, endpoints, *slave)
	if !assert.NoError(t, err) {
		cancel()
		return
	}
	log.Debug("Sending invalid value to slave")
	writeConfigETCD(endpoints, slaveKey, []byte("derp"))
	// allow time to listen for the updates
	time.Sleep(1 * time.Millisecond)

	cancel()
}

// writeConfigETCD sets data to etcd cluster with the given key and data
func writeConfigETCD(endpoints []string, key string, data []byte) error {
	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()

	// set value
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = cli.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("could not send tlog config: %v", err)
	}

	return nil
}

func writeNBDConfigETCD(vdiskID string, endpoints []string, nbd NBDConfig) error {
	nbdKey := etcdNBDKey(vdiskID)

	// validate before sending
	//read base for vdisk type (validation)
	base, err := ReadBaseConfigETCD(vdiskID, endpoints)
	if err != nil {
		return fmt.Errorf("could not get BaseConfig for writing to NBDConfig: %s", err)
	}
	err = nbd.Validate(base.Type)
	if err != nil {
		return fmt.Errorf("trying to write invalid config: %s", err)
	}
	nbdBS, err := nbd.ToBytes()
	if err != nil {
		return fmt.Errorf("could not turn NBDConfig to byteslice before sending to etcd: %s", err)
	}

	return writeConfigETCD(endpoints, nbdKey, nbdBS)
}

func writeTlogConfigETCD(vdiskID string, endpoints []string, tlog TlogConfig) error {
	tlogKey := etcdTlogKey(vdiskID)

	// validate before sending
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("trying to write invalid config to %s: %s", tlogKey, err)
	}

	tlogBS, err := tlog.ToBytes()
	if err != nil {
		return fmt.Errorf("could not turn TlogConfig to byteslice before sending to etcd: %s", err)
	}

	return writeConfigETCD(endpoints, tlogKey, tlogBS)
}

func writeSlaveConfigETCD(vdiskID string, endpoints []string, slave SlaveConfig) error {
	slaveKey := etcdSlaveKey(vdiskID)

	// validate before sending
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("trying to write invalid configto %s: %s", slaveKey, err)
	}

	slaveBS, err := slave.ToBytes()
	if err != nil {
		return fmt.Errorf("could not turn SlaveConfig to byteslice before sending to etcd: %s", err)
	}

	return writeConfigETCD(endpoints, slaveKey, slaveBS)
}

func cleanupETCD(cli *clientv3.Client, baseKey, nbdKey, tlogKey, slaveKey string) error {
	// cleanup data
	ops := []clientv3.Op{
		clientv3.OpDelete(baseKey),
		clientv3.OpDelete(nbdKey),
		clientv3.OpDelete(tlogKey),
		clientv3.OpDelete(slaveKey),
	}

	for _, op := range ops {
		_, err := cli.Do(context.TODO(), op)
		if err != nil {
			return err
		}
	}
	return nil
}
