package config

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/log"
)

const (
	dialTimeout = 5 * time.Second
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
func ReadNBDConfigETCD(vdiskID string, endpoints []string) (*BaseConfig, *NBDConfig, error) {
	nbdKey := etcdNBDKey(vdiskID)
	// Read base for vdisk type (validation)
	base, err := ReadBaseConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get BaseConfig for NBDConfig: %s", err)
	}

	// get nbd data from ETCD
	nbdBS, err := readConfigETCD(endpoints, nbdKey)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get NBDConfig from etcd: %s", err)
	}

	// parse NBD Config
	nbd, err := NewNBDConfig(nbdBS, base.Type)
	if err != nil {
		return nil, nil, err
	}

	// return base and nbd
	return base, nbd, nil
}

// WatchNBDConfigETCD watches etcd for NBDConfig updates
// sends the current config to the channel when created
func WatchNBDConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan NBDConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	base, nbd, err := ReadNBDConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial NBDconfig for NBdConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan NBDConfig, 1)
	updater <- *nbd

	err = watchConfigETCD(ctx, endpoints, etcdNBDKey(vdiskID), func(data []byte) error {
		nbd, err := NewNBDConfig(data, base.Type)
		if err != nil {
			return err
		}

		// send current data to channel
		select {
		case updater <- *nbd:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create NBdConfig watcher: %s", err)
	}

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

// WatchTlogConfigETCD watches etcd for TlogConfig updates
// sends the current config to the channel when created
func WatchTlogConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan TlogConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	tlog, err := ReadTlogConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial TlogConfig for TlogConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan TlogConfig, 1)
	updater <- *tlog

	err = watchConfigETCD(ctx, endpoints, etcdTlogKey(vdiskID), func(data []byte) error {
		tlog, err := NewTlogConfig(data)
		if err != nil {
			return err
		}

		// send current data to channel
		select {
		case updater <- *tlog:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create TlogConfig watcher: %s", err)
	}

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

// WatchSlaveConfigETCD watches etcd for SlaveConfig updates
// sends the current config to the channel when created
func WatchSlaveConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan SlaveConfig, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// fetch current data
	slave, err := ReadSlaveConfigETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf("Could not fetch initial SlaveConfig for SlaveConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan SlaveConfig, 1)
	updater <- *slave

	err = watchConfigETCD(ctx, endpoints, etcdSlaveKey(vdiskID), func(data []byte) error {
		slave, err := NewSlaveConfig(data)
		if err != nil {
			return err
		}

		// send current data to channel
		select {
		case updater <- *slave:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create SlaveConfig watcher: %s", err)
	}

	return updater, nil
}

func watchConfigETCD(ctx context.Context, endpoints []string, keyPrefix string, useConfig func(bytes []byte) error) error {
	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Errorf("could not connect to ETCD server: %v", err)
		return err
	}
	defer cli.Close()

	watch := cli.Watch(ctx, keyPrefix, clientv3.WithPrefix())

	log.Debugf("watch channel for %s started", keyPrefix)

	go func() {
		defer log.Debugf("watch channel for %s closed", keyPrefix)
		for {
			select {
			case <-ctx.Done():
				return

			case resp, ok := <-watch:
				if !ok || resp.Err() != nil {
					if ok {
						err := resp.Err()
						log.Errorf("Watch channel for %s encountered an error: %v", keyPrefix, err)
					}
					return
				}

				// get latest event
				ev := resp.Events[len(resp.Events)-1]
				log.Debugf("Value for %s received an update", ev.Kv.Key)

				// check if empty, if so log an error
				if len(ev.Kv.Value) < 1 {
					log.Errorf("key %s returned an empty value, keeping the old config", keyPrefix)
					continue
				}

				err = useConfig(ev.Kv.Value)
				if err != nil {
					log.Errorf("Watch channel for %s encountered an error: %v", keyPrefix, err)
				}
			}
		}
	}()

	return nil
}

// readConfigETCD fetches data from etcd cluster with the given key
func readConfigETCD(endpoints []string, key string) ([]byte, error) {
	// setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
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
