package configV2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/log"
)

// BaseConfigFromETCD return BaseConfig from ETCD source
func BaseConfigFromETCD(vdiskID string, endpoints []string) (*BaseConfig, error) {
	baseKey := etcdBaseKey(vdiskID)
	// Setup connection
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()
	resp, err := cli.Get(context.TODO(), baseKey)
	if err != nil {
		return nil, fmt.Errorf("could not get base from ETCD: %v", err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("base config was not found on the ETCD server")
	}
	base, err := NewBaseConfig(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return base, nil
}

//NBDConfigFromETCD gets an NBDConfig from etcd cluster
func NBDConfigFromETCD(vdiskID string, endpoints []string, vdiskType VdiskType) (*NBDConfig, error) {
	// Setup connection
	nbdKey := etcdNBDKey(vdiskID)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()

	// fetch nbd
	resp, err := cli.Get(context.TODO(), nbdKey)
	if err != nil {
		return nil, fmt.Errorf("could not get nbd from ETCD: %v", err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("nbd config was not found on the ETCD server")
	}
	nbd, err := NewNBDConfig(resp.Kvs[0].Value, vdiskType)
	if err != nil {
		return nil, err
	}

	return nbd, err
}

// TlogConfigFromETCD returns the TlogConfig from an etcd cluster
func TlogConfigFromETCD(vdiskID string, endpoints []string) (*TlogConfig, error) {
	// Setup connection
	tlogKey := etcdTlogKey(vdiskID)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()

	// fetch tlog
	resp, err := cli.Get(context.TODO(), tlogKey)
	if err != nil {
		return nil, fmt.Errorf("could not get tlog from ETCD: %v", err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("tlog config was not found on the ETCD server")
	}
	tlog, err := NewTlogConfig(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return tlog, err
}

// SlaveConfigFromETCD returns the SlaveConfig from an etcd cluster
func SlaveConfigFromETCD(vdiskID string, endpoints []string) (*SlaveConfig, error) {
	// Setup connection
	slaveKey := etcdSlaveKey(vdiskID)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}
	defer cli.Close()

	// fetch slave
	resp, err := cli.Get(context.TODO(), slaveKey)
	if err != nil {
		return nil, fmt.Errorf("could not get slave from ETCD: %v", err)
	}
	if len(resp.Kvs) < 1 {
		return nil, fmt.Errorf("slave config was not found on the ETCD server")
	}
	slave, err := NewSlaveConfig(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return slave, err
}

type etcdNBDConfig struct {
	vdiskType VdiskType
	key       string
	nbd       *NBDConfig
	lock      sync.RWMutex
	cli       *clientv3.Client
	subs      map[chan<- NBDConfig]struct{}
	subLock   sync.RWMutex
}

// NBDConfigETCDSource returns a NBDConfigSource implementation for etcd
func NBDConfigETCDSource(vdiskID string, endpoints []string, vdiskType VdiskType) (NBDConfigSource, error) {
	nbd, err := NBDConfigFromETCD(vdiskID, endpoints, vdiskType)
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup etcdNBDConfig
	enbd := new(etcdNBDConfig)
	enbd.lock.Lock()
	enbd.nbd = nbd
	enbd.lock.Unlock()
	enbd.vdiskType = vdiskType
	enbd.key = etcdNBDKey(vdiskID)
	enbd.cli = cli
	enbd.subs = make(map[chan<- NBDConfig]struct{})

	enbd.watch()

	return enbd, nil
}

// Close closes etcd connection
func (enbd *etcdNBDConfig) Close() error {
	return enbd.cli.Close()
}

func (enbd *etcdNBDConfig) SetNBDConfig(nbd NBDConfig) error {
	// validate input
	err := nbd.Validate(enbd.vdiskType)
	if err != nil {
		return fmt.Errorf("provided nbd config was not valid: %s", err)
	}

	// save locally
	enbd.lock.Lock()
	enbd.nbd = nbd.Clone()
	enbd.lock.Unlock()

	// write to etcd
	val, err := nbd.ToBytes()
	if err != nil {
		return err
	}
	_, err = enbd.cli.Put(context.TODO(), enbd.key, string(val))
	if err != nil {
		return fmt.Errorf("could not send nbd config: %v", err)
	}
	return nil
}

// Returns the latest clone of NBD config version
func (enbd *etcdNBDConfig) NBDConfig() (*NBDConfig, error) {
	enbd.lock.RLock()
	defer enbd.lock.RUnlock()
	return enbd.nbd.Clone(), nil
}

// return the NBD's VdiskType
func (enbd *etcdNBDConfig) VdiskType() VdiskType {
	return enbd.vdiskType
}

func (enbd *etcdNBDConfig) Subscribe(c chan<- NBDConfig) error {
	if c == nil {
		return fmt.Errorf("can't subscribe to %s with nil channel", enbd.key)
	}
	enbd.subLock.Lock()
	enbd.subs[c] = struct{}{}
	enbd.subLock.Unlock()

	return nil
}

func (enbd *etcdNBDConfig) Unsubscribe(c chan<- NBDConfig) error {
	if c == nil {
		return fmt.Errorf("can't unsubscribe to %s with nil channel", enbd.key)
	}

	enbd.subLock.Lock()
	delete(enbd.subs, c)
	enbd.subLock.Unlock()

	return nil
}

func (enbd *etcdNBDConfig) watch() {
	watch := enbd.cli.Watch(context.TODO(), enbd.key, clientv3.WithPrefix())
	go func() {
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", enbd.key, resp.Err())
				}
				log.Infof("Watch channel for %s closed", enbd.key)
				return
			}
			for _, ev := range resp.Events {
				log.Infof("Value for %s received an update", ev.Kv.Key)

				// check if empty, if so make the subconfig nil
				if len(ev.Kv.Value) < 1 {
					enbd.lock.Lock()
					enbd.nbd = nil
					enbd.lock.Unlock()
					log.Infof("%s was found empty on ETCD, setting nbd to nil", ev.Kv.Key)
					continue
				}

				// create new NBDConfig
				nbd, err := NewNBDConfig(ev.Kv.Value, enbd.vdiskType)
				if err != nil {
					log.Errorf("did not update nbd for %s: %s", enbd.key, err)
					continue
				}

				// if valid save it and return to channel
				enbd.lock.Lock()
				enbd.nbd = nbd
				enbd.lock.Unlock()
				// send value to each subscriber
				enbd.subLock.RLock()
				for sub := range enbd.subs {
					sub <- *nbd.Clone()
				}
				enbd.subLock.RUnlock()
			}
		}
	}()
}

type etcdTlogConfig struct {
	tlog    *TlogConfig
	lock    sync.RWMutex
	cli     *clientv3.Client
	key     string
	subs    map[chan<- TlogConfig]struct{}
	subLock sync.RWMutex
}

// TlogConfigETCDSource returns a TlogConfigSource implementation for etcd
func TlogConfigETCDSource(vdiskID string, endpoints []string) (TlogConfigSource, error) {
	tlog, err := TlogConfigFromETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup etcdtlogConfig
	etlog := new(etcdTlogConfig)
	etlog.lock.Lock()
	etlog.tlog = tlog
	etlog.lock.Unlock()
	etlog.key = etcdTlogKey(vdiskID)
	etlog.cli = cli
	etlog.subs = make(map[chan<- TlogConfig]struct{})

	// launch watch
	etlog.watch()

	return etlog, nil
}

// Close closes etcd connection
func (etlog *etcdTlogConfig) Close() error {
	return etlog.cli.Close()
}

func (etlog *etcdTlogConfig) SetTlogConfig(tlog TlogConfig) error {
	// validate input
	err := tlog.Validate()
	if err != nil {
		return fmt.Errorf("provided nbd config was not valid: %s", err)
	}

	// save locally
	etlog.lock.Lock()
	etlog.tlog = tlog.Clone()
	etlog.lock.Unlock()

	// write to etcd
	val, err := tlog.ToBytes()
	if err != nil {
		return err
	}
	_, err = etlog.cli.Put(context.TODO(), etlog.key, string(val))
	if err != nil {
		return fmt.Errorf("could not send tlog config: %v", err)
	}
	return nil
}

// Returns the latest Tlog config version
func (etlog *etcdTlogConfig) TlogConfig() (*TlogConfig, error) {
	etlog.lock.RLock()
	defer etlog.lock.RUnlock()
	return etlog.tlog.Clone(), nil
}

func (etlog *etcdTlogConfig) Subscribe(c chan<- TlogConfig) error {
	if c == nil {
		return fmt.Errorf("can't subscribe to %s with nil channel", etlog.key)
	}
	etlog.subLock.Lock()
	etlog.subs[c] = struct{}{}
	etlog.subLock.Unlock()

	return nil
}

func (etlog *etcdTlogConfig) Unsubscribe(c chan<- TlogConfig) error {
	if c == nil {
		return fmt.Errorf("can't unsubscribe to %s with nil channel", etlog.key)
	}

	etlog.subLock.Lock()
	delete(etlog.subs, c)
	etlog.subLock.Unlock()

	return nil
}

func (etlog *etcdTlogConfig) watch() {
	watch := etlog.cli.Watch(context.TODO(), etlog.key, clientv3.WithPrefix())
	go func() {
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", etlog.key, resp.Err())
				}
				log.Infof("Watch channel for %s closed", etlog.key)
				return
			}
			for _, ev := range resp.Events {
				log.Infof("Value for %s received an update", ev.Kv.Key)

				// check if empty, if so make the subconfig nil
				if len(ev.Kv.Value) < 1 {
					etlog.lock.Lock()
					etlog.tlog = nil
					etlog.lock.Unlock()
					log.Infof("%s was found empty on ETCD, setting tlog to nil", ev.Kv.Key)
					continue
				}

				// create new TlogConfig
				tlog, err := NewTlogConfig(ev.Kv.Value)
				if err != nil {
					log.Errorf("did not update tlog for %s: %s", etlog.key, err)
					continue
				}

				// if valid save it and return to channel
				etlog.lock.Lock()
				etlog.tlog = tlog
				etlog.lock.Unlock()
				// send value to each subscriber
				etlog.subLock.RLock()
				for sub := range etlog.subs {
					sub <- *tlog.Clone()
				}
				etlog.subLock.RUnlock()
			}
		}
	}()
}

type etcdSlaveConfig struct {
	slave   *SlaveConfig
	lock    sync.RWMutex
	cli     *clientv3.Client
	key     string
	subs    map[chan<- SlaveConfig]struct{}
	subLock sync.RWMutex
}

// SlaveConfigETCDSource returns a SlaveConfigSource implementation for etcd
func SlaveConfigETCDSource(vdiskID string, endpoints []string) (SlaveConfigSource, error) {
	slave, err := SlaveConfigFromETCD(vdiskID, endpoints)
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to ETCD server: %v", err)
	}

	// setup etcdslaveConfig
	eslave := new(etcdSlaveConfig)
	eslave.lock.Lock()
	eslave.slave = slave
	eslave.lock.Unlock()
	eslave.key = etcdSlaveKey(vdiskID)
	eslave.cli = cli
	eslave.subs = make(map[chan<- SlaveConfig]struct{})

	// launch watch
	eslave.watch()

	return eslave, nil
}

// Close closes etcd connection
func (eslave *etcdSlaveConfig) Close() error {
	return eslave.cli.Close()
}

func (eslave *etcdSlaveConfig) SetSlaveConfig(slave SlaveConfig) error {
	// validate input
	err := slave.Validate()
	if err != nil {
		return fmt.Errorf("provided slave config was not valid: %s", err)
	}

	// save locally
	eslave.lock.Lock()
	eslave.slave = slave.Clone()
	eslave.lock.Unlock()

	// write to etcd
	val, err := slave.ToBytes()
	if err != nil {
		return err
	}
	_, err = eslave.cli.Put(context.TODO(), eslave.key, string(val))
	if err != nil {
		return fmt.Errorf("could not send slave config: %v", err)
	}
	return nil
}

// Returns the latest Slave config version
func (eslave *etcdSlaveConfig) SlaveConfig() (*SlaveConfig, error) {
	eslave.lock.RLock()
	defer eslave.lock.RUnlock()
	return eslave.slave.Clone(), nil
}

func (eslave *etcdSlaveConfig) Subscribe(c chan<- SlaveConfig) error {
	if c == nil {
		return fmt.Errorf("can't subscribe to %s with nil channel", eslave.key)
	}
	eslave.subLock.Lock()
	eslave.subs[c] = struct{}{}
	eslave.subLock.Unlock()

	return nil
}

func (eslave *etcdSlaveConfig) Unsubscribe(c chan<- SlaveConfig) error {
	if c == nil {
		return fmt.Errorf("can't unsubscribe to %s with nil channel", eslave.key)
	}

	eslave.subLock.Lock()
	delete(eslave.subs, c)
	eslave.subLock.Unlock()

	return nil
}

func (eslave *etcdSlaveConfig) watch() {
	watch := eslave.cli.Watch(context.TODO(), eslave.key, clientv3.WithPrefix())
	go func() {
		for {
			resp, ok := <-watch
			if !ok || resp.Err() != nil {
				if ok {
					log.Errorf("Watch channel for %s encountered an error: %v", eslave.key, resp.Err())
				}
				log.Infof("Watch channel for %s closed", eslave.key)
				return
			}
			for _, ev := range resp.Events {
				log.Infof("Value for %s received an update", ev.Kv.Key)

				// check if empty, if so make the subconfig nil
				if len(ev.Kv.Value) < 1 {
					eslave.lock.Lock()
					eslave.slave = nil
					eslave.lock.Unlock()
					log.Infof("%s was found empty on ETCD, setting slave to nil", ev.Kv.Key)
					continue
				}

				// create new SlaveConfig
				slave, err := NewSlaveConfig(ev.Kv.Value)
				if err != nil {
					log.Errorf("did not update slave for %s: %s", eslave.key, err)
					continue
				}

				// if valid save it and return to channel
				eslave.lock.Lock()
				eslave.slave = slave
				eslave.lock.Unlock()
				// send value to each subscriber
				eslave.subLock.RLock()
				for sub := range eslave.subs {
					sub <- *slave.Clone()
				}
				eslave.subLock.RUnlock()
			}
		}
	}()
}

// etcdBaseKey returns base key for provided vdiskid
// update the watcher if keys are changed
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
