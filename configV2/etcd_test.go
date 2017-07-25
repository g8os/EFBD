package configV2

import (
	"context"
	"testing"
	"time"

	"fmt"

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

	base, err := BaseConfigFromETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, base) {
		return
	}

	ba, _ := NewBaseConfig([]byte(validBaseStr))
	if !assert.Equal(t, ba.BlockSize, base.BlockSize) {
		return
	}

	nbd, err := NBDConfigFromETCD(vdiskID, endpoints, base.Type)
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd) {
		return
	}

	tlog, err := TlogConfigFromETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}

	slave, err := SlaveConfigFromETCD(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, slave) {
		return
	}

	nbdSrc, err := NBDConfigETCDSource(vdiskID, endpoints, base.Type)
	if !assert.NoError(t, err) || !assert.NotNil(t, nbdSrc) {
		return
	}
	defer nbdSrc.Close()

	nbdChan := make(chan NBDConfig)
	err = nbdSrc.Subscribe(nbdChan)
	if !assert.NoError(t, err) {
		return
	}

	err = nbdSrc.SetNBDConfig(*nbd)
	if !assert.NoError(t, err) {
		return
	}

	resp, ok := <-nbdChan
	if !assert.True(t, ok) || !assert.NotNil(t, resp) {
		return
	}

	err = nbdSrc.Unsubscribe(nbdChan)
	if !assert.NoError(t, err) {
		return
	}

	err = nbdSrc.SetNBDConfig(*nbd)
	if !assert.NoError(t, err) {
		return
	}

	tlogSrc, err := TlogConfigETCDSource(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, tlogSrc) {
		return
	}
	defer tlogSrc.Close()

	slaveSrc, err := SlaveConfigETCDSource(vdiskID, endpoints)
	if !assert.NoError(t, err) || !assert.NotNil(t, slaveSrc) {
		return
	}
	slaveSrc.Close()

	fmt.Println("end slave")

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
