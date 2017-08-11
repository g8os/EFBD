package config

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/log"
)

// ETCDV3Source creates a config source,
// where the configurations originate from an ETCD v3 cluster.
func ETCDV3Source(endpoints []string) (SourceCloser, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		log.Errorf("ETCDV3Source requires valid etcd v3 client: %v", err)
		log.Broadcast(
			log.StatusClusterTimeout,
			log.SubjectETCD,
			endpoints,
		)
		return nil, ErrSourceUnavailable
	}

	return &etcdv3Source{
		client:    client,
		endpoints: endpoints,
	}, nil
}

type etcdv3Source struct {
	client    *clientv3.Client
	endpoints []string
}

// Get implements Source.Get
func (s *etcdv3Source) Get(key Key) ([]byte, error) {
	// create ctx
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// convert our internal key type to an etcd key
	keyString, err := ETCDKey(key.ID, key.Type)
	if err != nil {
		log.Errorf("invalid config key: %v", err)
		return nil, ErrInvalidKey
	}

	// get value
	resp, err := s.client.Get(ctx, keyString)
	if err != nil {
		// TODO: could this also be because we lost connection to the ETCD cluster?
		log.Errorf("could not get key '%s' from ETCD: %v", keyString, err)
		return nil, ErrConfigUnavailable
	}

	// ensure value was found
	if len(resp.Kvs) < 1 {
		log.Errorf("key '%s' was not found on the ETCD server", keyString)
		return nil, ErrConfigUnavailable
	}

	// return the value
	return resp.Kvs[0].Value, nil
}

// Watch implements Source.Watch
func (s *etcdv3Source) Watch(ctx context.Context, key Key) (<-chan []byte, error) {
	// convert our internal key type to an etcd key
	keyString, err := ETCDKey(key.ID, key.Type)
	if err != nil {
		log.Errorf("invalid config key: %v", err)
		return nil, ErrInvalidKey
	}

	ctx, cancel := context.WithCancel(ctx)
	watch := s.client.Watch(ctx, keyString)

	ch := make(chan []byte, 1)

	go func() {
		log.Debugf("watch goroutine for etcd key '%s' started", keyString)
		defer cancel()
		defer log.Debugf("watch goroutine for etcd key '%s' closed", keyString)
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return

			case resp, open := <-watch:
				if !open {
					log.Debugf("watch channel for etcd key %s has closed, re-opening it", keyString)
					watch = s.client.Watch(ctx, keyString)
					continue
				}

				if resp.IsProgressNotify() {
					log.Debugf("watch channel for etcd key %s skipping progress notification", keyString)
					continue
				}

				var bytes []byte

				// we want to send an update, no matter if the value is invalid,
				// as it gives us a chance to mark the key as invalid
				if err := resp.Err(); err != nil {
					log.Errorf(
						"watch channel for key '%s' encountered an error: %v", keyString, err)
				} else if len(resp.Events) == 0 {
					log.Errorf("key '%s' was not found on the ETCD server", keyString)
				} else {
					ev := resp.Events[len(resp.Events)-1]
					if ev.Kv != nil {
						bytes = ev.Kv.Value
					} else {
						log.Errorf("key '%s' was found, but value was nil", keyString)
					}
				}

				log.Debugf("value for %s received an update", keyString)

				// send (updated) value
				select {
				case ch <- bytes:
				case <-ctx.Done():
					log.Errorf(
						"timed out while attempting to send updated config (%s)", keyString)
				}
			}
		}
	}()

	// watch function active
	return ch, nil
}

// MarkInvalidKey implements Source.MarkInvalidKey
func (s *etcdv3Source) MarkInvalidKey(key Key, vdiskID string) {
	keyStr, err := ETCDKey(key.ID, key.Type)
	if err != nil {
		panic(err) // should never happen
	}

	log.Errorf(
		"received invalid etcd config '%s' (vdisk:'%s')", keyStr, vdiskID)

	log.Broadcast(
		log.StatusInvalidConfig,
		log.SubjectETCD,
		log.InvalidConfigBody{
			Endpoints: s.endpoints,
			Key:       keyStr,
			VdiskID:   vdiskID,
		},
	)
}

// SourceConfig implements Source.SourceConfig
func (s *etcdv3Source) SourceConfig() interface{} {
	return s.endpoints
}

// Type implements Source.Type
func (s *etcdv3Source) Type() string {
	return "etcd"
}

// Close implements Source.Close
func (s *etcdv3Source) Close() error {
	return s.client.Close()
}

// ETCDKey returns the etcd key for a given type and unique id.
func ETCDKey(id string, ktype KeyType) (string, error) {
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
