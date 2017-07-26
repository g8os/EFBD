package configV2

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/log"
)

const (
	dialTM = 5 * time.Second
)

// ReadBaseConfigETCD gets BaseConfig from provided etcd cluster
func ReadBaseConfigETCD(vdiskID string, endpoints []string) (*BaseConfig, error) {
	baseKey := etcdBaseKey(vdiskID)

	baseBS, err := readConfigETCD(endpoints, baseKey)
	if err != nil {
		return nil, err
	}

	return NewBaseConfig(baseBS)
}

//ReadNBDConfigETCD gets an NBDConfig from provided etcd cluster
func ReadNBDConfigETCD(vdiskID string, endpoints []string) (*NBDConfig, error) {
	nbdKey := etcdNBDKey(vdiskID)
	// Read base for vdisk type (validation)
	base, err := ReadBaseConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("could not get BaseConfig for NBDConfig: %s", err)
	}

	// get nbd data from ETCD
	nbdBS, err := readConfigETCD(endpoints, nbdKey)
	if err != nil {
		return nil, fmt.Errorf("could not get NBDConfig from etcd: %s", err)
	}

	// return unmarshalled NBDConfig
	return NewNBDConfig(nbdBS, base.Type)
}

// WriteNBDConfigETCD sets an NBDConfig to provided etcd cluster
func WriteNBDConfigETCD(vdiskID string, endpoints []string, nbd NBDConfig) error {
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

// NBDConfigResult represents a NBDConfig watch result
type NBDConfigResult struct {
	NBD *NBDConfig
	Err error
}

// WatchNBDConfigETCD watches etcd for NBDConfig updates
func WatchNBDConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan NBDConfigResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	nbdKey := etcdNBDKey(vdiskID)

	// fetch current data
	base, err := ReadBaseConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not get BaseConfig for NBDConfig watcher: %s", err)
	}
	// get nbd data from ETCD
	nbdBS, err := readConfigETCD(endpoints, nbdKey)
	if err != nil {
		return nil, fmt.Errorf("could not get NBDConfig from etcd: %s", err)
	}

	nbd, err := NewNBDConfig(nbdBS, base.Type)
	if err != nil {
		return nil, fmt.Errorf("NBDConfig on etcd is not valid: %s", err)
	}

	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTM,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup channel
	updater := make(chan NBDConfigResult, 1)

	// send current data to channel
	updater <- NBDConfigResult{
		NBD: nbd,
		Err: nil,
	}

	// watch for updates
	go func() {
		defer cli.Close()
		defer close(updater)
		watch := cli.Watch(ctx, nbdKey, clientv3.WithPrefix())
		for {
			resp, ok := <-watch
			// if user uses cancelFunc of his provided context
			// it will close the watch client channel
			// checking for this will close this goroutine
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", nbdKey, resp.Err())
				}
				log.Infof("Closing watcher for %s", nbdKey)
				return
			}
			// get latest event
			ev := resp.Events[len(resp.Events)-1]
			log.Debugf("Value for %s received an update", ev.Kv.Key)

			// check if empty, if so log an error
			if len(ev.Kv.Value) < 1 {
				log.Errorf("key %s returned an empty value, keeping the old config", ev.Kv.Key)
				continue
			}

			// create new NBDConfig
			nbd, err := NewNBDConfig(ev.Kv.Value, base.Type)
			if err != nil {
				log.Errorf("did not update nbd for %s: %s", ev.Kv.Key, err)
				continue
			}

			// send value to channel
			updater <- NBDConfigResult{
				NBD: nbd,
				Err: nil,
			}
		}
	}()

	return updater, nil
}

// ReadTlogConfigETCD returns the TlogConfig from provided etcd cluster
func ReadTlogConfigETCD(vdiskID string, endpoints []string) (*TlogConfig, error) {
	tlogKey := etcdTlogKey(vdiskID)

	tlogBS, err := readConfigETCD(endpoints, tlogKey)
	if err != nil {
		return nil, err
	}

	return NewTlogConfig(tlogBS)
}

// WriteTlogConfigETCD sets a TlogConfig to provided etcd cluster
func WriteTlogConfigETCD(vdiskID string, endpoints []string, tlog TlogConfig) error {
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

// TlogConfigResult represents a TlogConfig watch result
type TlogConfigResult struct {
	Tlog *TlogConfig
	Err  error
}

// WatchTlogConfigETCD watches etcd for TlogConfig updates
func WatchTlogConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan TlogConfigResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tlogKey := etcdTlogKey(vdiskID)

	// fetch current data
	tlog, err := ReadTlogConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not get BaseConfig for TlogConfig watcher: %s", err)
	}

	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTM,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup channel
	updater := make(chan TlogConfigResult, 1)

	// send current data to channel
	updater <- TlogConfigResult{
		Tlog: tlog,
		Err:  nil,
	}

	// watch for updates
	go func() {
		defer cli.Close()
		defer close(updater)
		watch := cli.Watch(ctx, tlogKey, clientv3.WithPrefix())
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", tlogKey, resp.Err())
				}
				log.Infof("Watch channel for %s closed", tlogKey)
				return
			}
			// get latest event
			ev := resp.Events[len(resp.Events)-1]
			log.Debugf("Value for %s received an update", ev.Kv.Key)

			// check if empty, if so log an error
			if len(ev.Kv.Value) < 1 {
				log.Errorf("key %s returned an empty value, keeping the old config", ev.Kv.Key)
				continue
			}

			// create new TlogConfig
			tlog, err := NewTlogConfig(ev.Kv.Value)
			if err != nil {
				log.Errorf("did not update tlog for %s: %s", ev.Kv.Key, err)
				continue
			}

			// send value to channel
			updater <- TlogConfigResult{
				Tlog: tlog,
				Err:  nil,
			}
		}
	}()

	return updater, nil
}

// ReadSlaveConfigETCD returns the SlaveConfig from provided etcd cluster
func ReadSlaveConfigETCD(vdiskID string, endpoints []string) (*SlaveConfig, error) {
	slaveKey := etcdSlaveKey(vdiskID)

	slaveBS, err := readConfigETCD(endpoints, slaveKey)
	if err != nil {
		return nil, err
	}

	return NewSlaveConfig(slaveBS)
}

// WriteSlaveConfigETCD sets a SlaveConfig to etcd cluster
func WriteSlaveConfigETCD(vdiskID string, endpoints []string, slave SlaveConfig) error {
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

// WatchSlaveConfigETCD watches etcd for SlaveConfig updates
func WatchSlaveConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan SlaveConfigResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	slaveKey := etcdSlaveKey(vdiskID)

	// fetch current data
	slave, err := ReadSlaveConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not get BaseConfig for SlaveConfig watcher: %s", err)
	}

	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTM,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup channel
	updater := make(chan SlaveConfigResult, 1)

	// send current data to channel
	updater <- SlaveConfigResult{
		Slave: slave,
		Err:   nil,
	}

	// watch for updates
	go func() {
		defer cli.Close()
		defer close(updater)
		watch := cli.Watch(ctx, slaveKey, clientv3.WithPrefix())
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", slaveKey, resp.Err())
				}
				log.Infof("Watch channel for %s closed", slaveKey)
				return
			}
			// get latest event
			ev := resp.Events[len(resp.Events)-1]
			log.Debugf("Value for %s received an update", ev.Kv.Key)

			// check if empty, if so log an error
			if len(ev.Kv.Value) < 1 {
				log.Errorf("key %s returned an empty value, keeping the old config", ev.Kv.Key)
				continue
			}

			// create new SlaveConfig
			slave, err := NewSlaveConfig(ev.Kv.Value)
			if err != nil {
				log.Errorf("did not update slave for %s: %s", ev.Kv.Key, err)
				continue
			}

			// send value to channel
			updater <- SlaveConfigResult{
				Slave: slave,
				Err:   nil,
			}
		}
	}()

	return updater, nil
}

// readConfigETCD fetches data from etcd cluster with the given key
func readConfigETCD(endpoints []string, key string) ([]byte, error) {
	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTM,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()

	// get value
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("could not get key '%s' from ETCD: %v", key, err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("key '%s' was not found on the ETCD server", key)
	}

	if len(resp.Kvs[0].Value) < 1 {
		return nil, fmt.Errorf("value for %s is empty", key)
	}

	return resp.Kvs[0].Value, nil
}

// writeConfigETCD sets data to etcd cluster with the given key and data
func writeConfigETCD(endpoints []string, key string, data []byte) error {
	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTM,
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

// SlaveConfigResult represents a SlaveConfig watch result
type SlaveConfigResult struct {
	Slave *SlaveConfig
	Err   error
}

// etcdBaseKey returns base key for provided vdiskid
func etcdBaseKey(vdiskID string) string {
	return vdiskID + ":conf:base"
}

// etcdNBDKey returns nbd key for provided vdiskid
func etcdNBDKey(vdiskID string) string {
	return vdiskID + ":conf:nbd"
}

// etcdTlogKey returns tlog key for provided vdiskid
func etcdTlogKey(vdiskID string) string {
	return vdiskID + ":conf:tlog"
}

// etcdSlaveKey returns slave key for provided vdiskid
func etcdSlaveKey(vdiskID string) string {
	return vdiskID + ":conf:slave"
}
