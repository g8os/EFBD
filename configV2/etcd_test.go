package configV2

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

// test only works if local etcd is running
func testETCDConfig(t *testing.T) {
	//init
	endpoints := []string{"127.0.0.1:2379"}
	vdiskID := "test"
	baseKey := etcdBaseKey(vdiskID)
	nbdKey := etcdNBDKey(vdiskID)
	tlogKey := etcdTlogKey(vdiskID)
	slaveKey := etcdSlaveKey(vdiskID)

	// input data
	cfg, err := fromYAMLBytes([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
	b, err := cfg.base.ToBytes()
	n, err := cfg.nbd.ToBytes()
	tl, err := cfg.tlog.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if !assert.NoError(t, err) || !assert.NotNil(t, cli) {
		return
	}
	defer cli.Close()
	// cleanup ETCD
	cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)

	ops := []clientv3.Op{
		clientv3.OpPut(baseKey, string(b)),
		clientv3.OpPut(nbdKey, string(n)),
		clientv3.OpPut(tlogKey, string(tl)),
	}

	for _, op := range ops {
		_, err := cli.Do(context.TODO(), op)
		if !assert.NoError(t, err) {
			return
		}
	}

	etcdCfg, err := NewETCDConfig(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, etcdCfg) {
		return
	}
	defer etcdCfg.Close()
	defer cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)
	// check if  input is same as output
	//base
	eb := etcdCfg.Base()
	ebs, err := eb.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, b, ebs) {
		return
	}
	//nbd
	en, _ := etcdCfg.NBD()
	ens, err := en.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, n, ens) {
		return
	}
	//tlog
	etl, _ := etcdCfg.Tlog()
	etls, err := etl.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, tl, etls) {
		return
	}
	//slave should be empty as it was not provided
	s, _ := etcdCfg.Slave()
	if !assert.Nil(t, s) {
		return
	}

	// test setting data
	slave, err := NewSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(t, err) {
		return
	}
	etcdCfg.SetSlave(*slave)

	var newSlave *SlaveConfig
	resp, err := cli.Get(context.TODO(), slaveKey)
	if !assert.NoError(t, err) {
		return
	}
	if len(resp.Kvs) > 0 {
		newSlave, err = NewSlaveConfig(resp.Kvs[0].Value)
		if !assert.NoError(t, err) {
			return
		}
	}
	if !assert.NotNil(t, newSlave) {
		return
	}
	slaveBytes, _ := slave.ToBytes()
	newSlaveBytes, _ := newSlave.ToBytes()
	if !assert.Equal(t, slaveBytes, newSlaveBytes) {
		return
	}

	// test watcher
	newMDSAddress := "1.2.3.4:5678"
	slave.SlaveStorageCluster.MetadataStorage.Address = newMDSAddress
	slaveBS, _ := slave.ToBytes()
	_, err = cli.Put(context.TODO(), slaveKey, string(slaveBS))
	if !assert.NoError(t, err) {
		return
	}

	// leave some time to update
	time.Sleep(5 * time.Millisecond)

	slaveNow, _ := etcdCfg.Slave()
	assert.Equal(t, newMDSAddress, slaveNow.SlaveStorageCluster.MetadataStorage.Address)

	// moar testing
	tlog, _ := etcdCfg.Tlog()
	if !assert.NotNil(t, tlog) {
		return
	}
	_, err = cli.Put(context.TODO(), tlogKey, "")
	if !assert.NoError(t, err) {
		return
	}

	// leave some time to update
	time.Sleep(5 * time.Millisecond)

	// empty string was sent so that should be an nil tlog in the config
	tlog, _ = etcdCfg.Tlog()
	if !assert.Nil(t, tlog) {
		return
	}

	_, err = cli.Put(context.TODO(), tlogKey, validTlogStr)
	if !assert.NoError(t, err) {
		return
	}

	// leave some time to update
	time.Sleep(5 * time.Millisecond)
	// tlog should not be empty anymore
	tlog, _ = etcdCfg.Tlog()
	if !assert.NotNil(t, tlog) {
		return
	}

	// clear test data from ETCD
	cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)

	// to see all watch responses delete events logged
	time.Sleep(5 * time.Millisecond)
}

func testCleanETCD(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379"}
	vdiskID := "test"
	baseKey := etcdBaseKey(vdiskID)
	nbdKey := etcdNBDKey(vdiskID)
	tlogKey := etcdTlogKey(vdiskID)
	slaveKey := etcdSlaveKey(vdiskID)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if !assert.NoError(t, err) {
		return
	}
	defer cli.Close()
	err = cleanupETCD(cli, baseKey, nbdKey, tlogKey, slaveKey)
	assert.NoError(t, err)
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
