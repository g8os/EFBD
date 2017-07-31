package config

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/siddontang/go/log"
)

// ETCDV3Source creates a config source,
// where the configurations originate from an ETCD v3 cluster.
func ETCDV3Source(endpoints []string) (Source, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"ETCDV3Source requires valid etcd v3 client: %v", err)
	}

	return &etcdv3Source{client}, nil
}

type etcdv3Source struct {
	client *clientv3.Client
}

// Get implements Source.Get
func (s *etcdv3Source) Get(key Key) ([]byte, error) {
	// create ctx
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// convert our internal key type to an etcd key
	keyString, err := etcdKey(key.ID, key.Type)
	if err != nil {
		return nil, err
	}

	// get value
	resp, err := s.client.Get(ctx, keyString)
	if err != nil {
		return nil, fmt.Errorf(
			"could not get key '%s' from ETCD: %v", keyString, err)
	}

	// ensure value was found
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf(
			"key '%s' was not found on the ETCD server", keyString)
	}
	if len(resp.Kvs[0].Value) < 1 {
		return nil, fmt.Errorf("value for %s is empty", keyString)
	}

	// return the value
	return resp.Kvs[0].Value, nil
}

// Watch implements Source.Watch
func (s *etcdv3Source) Watch(ctx context.Context, key Key, cb WatchCallback) error {
	if cb == nil {
		return ErrNilCallback
	}

	// convert our internal key type to an etcd key
	keyString, err := etcdKey(key.ID, key.Type)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	watch := s.client.Watch(ctx, keyString)

	log.Debugf("watch channel for etcd key '%s' started", keyString)
	go func() {
		defer cancel()
		defer log.Debugf("watch channel for etcd key '%s' closed", keyString)

		for {
			select {
			case <-ctx.Done():
				return

			case resp, ok := <-watch:
				if !ok || resp.Err() != nil {
					if ok {
						err := resp.Err()
						log.Errorf(
							"Watch channel for key '%s' encountered an error: %v",
							keyString, err)
					}
					return
				}

				// get latest event
				ev := resp.Events[len(resp.Events)-1]
				log.Debugf("value for %s received an update", ev.Kv.Key)

				// check if empty, if so log an error
				if len(ev.Kv.Value) < 1 {
					log.Errorf(
						"key '%s' returned an empty value, keeping the old config",
						keyString)
					continue
				}

				err = cb(ev.Kv.Value)
				if err != nil {
					log.Errorf(
						"watch channel for '%s' encountered an error: %v",
						keyString, err)
				}
			}
		}
	}()

	// watch function active
	return nil
}

// Close implements Source.Close
func (s *etcdv3Source) Close() error {
	return s.client.Close()
}

func etcdKey(id string, ktype KeyType) (string, error) {
	suffix, ok := etcdKeySuffixes[ktype]
	if !ok {
		return "", fmt.Errorf("%v is not a supported key type", ktype)
	}
	return id + suffix, nil
}

var etcdKeySuffixes = map[KeyType]string{
	KeyVdiskStatic:     ":vdisk:conf:static",
	KeyVdiskNBD:        ":vdisk:conf:storage:nbd",
	KeyVdiskTlog:       ":vdisk:conf:storage:tlog",
	KeyClusterStorage:  ":cluster:conf:storage",
	KeyClusterTlog:     ":cluster:conf:tlog",
	KeyNBDServerVdisks: ":nbdserver:conf:vdisks",
}

const (
	etcdDialTimeout = 5 * time.Second
)
