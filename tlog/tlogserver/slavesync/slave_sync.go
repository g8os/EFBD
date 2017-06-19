package slavesync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/config"
	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

// Manager defines slave syncer manager
type Manager struct {
	apMq       *aggmq.MQ
	syncers    map[string]*slaveSyncer
	configPath string
	mux        sync.Mutex
}

// NewManager creates new slave syncer manager
func NewManager(apMq *aggmq.MQ, configPath string) *Manager {
	m := &Manager{
		apMq:       apMq,
		configPath: configPath,
		syncers:    make(map[string]*slaveSyncer),
	}
	return m
}

// Run runs the slave syncer manager
func (m *Manager) Run() {
	for {
		apr := <-m.apMq.NeedProcessorCh
		m.handleReq(apr)
	}
}

// handle request
func (m *Manager) handleReq(apr aggmq.AggProcessorReq) {
	m.mux.Lock()
	defer m.mux.Unlock()

	log.Debugf("slavesync manager : create for vdisk: %v", apr.Config.VdiskID)

	// check if the syncer already exist
	if _, ok := m.syncers[apr.Config.VdiskID]; ok {
		m.apMq.NeedProcessorResp <- nil
		return
	}

	// create slave syncer
	ss, err := newSlaveSyncer(apr.Context, m.configPath, apr.Config, apr.Comm, m)
	if err != nil {
		log.Errorf("failed to create slave syncer: %v", err)
		m.apMq.NeedProcessorResp <- err
		return
	}
	m.syncers[apr.Config.VdiskID] = ss

	// send response
	m.apMq.NeedProcessorResp <- nil

	log.Debugf("slave syncer created for vdisk: %v", apr.Config.VdiskID)
}

// remove slave syncer for given vdiskID
func (m *Manager) remove(vdiskID string) {
	m.mux.Lock()
	defer m.mux.Unlock()

	delete(m.syncers, vdiskID)
}

// slaveSyncer defines a tlog slave syncer
type slaveSyncer struct {
	ctx              context.Context
	vdiskID          string
	aggComm          *aggmq.AggComm // the communication channel
	player           *player.Player // tlog replay player
	mgr              *Manager
	dec              *decoder.Decoder            // tlog decoder
	metaPool         *ardb.RedisPool             // ardb meta redis pool
	metaConf         *config.StorageServerConfig // ardb meta redis config
	lastSeqSyncedKey string                      // key of the last sequence synced
}

// newSlaveSyncer creates a new slave syncer
func newSlaveSyncer(ctx context.Context, configPath string, apc aggmq.AggProcessorConfig,
	aggComm *aggmq.AggComm, mgr *Manager) (*slaveSyncer, error) {

	// create config object from empty string
	// we need this to create nbd backend
	serverConfigs, err := zerodiskcfg.ParseCSStorageServerConfigStrings("")
	if err != nil {
		return nil, err
	}

	// tlog replay player
	player, err := player.NewPlayer(ctx, configPath, serverConfigs, apc.VdiskID, apc.PrivKey,
		apc.HexNonce, apc.K, apc.M)
	if err != nil {
		return nil, err
	}

	ss := &slaveSyncer{
		vdiskID:          apc.VdiskID,
		ctx:              ctx,
		aggComm:          aggComm,
		mgr:              mgr,
		player:           player,
		lastSeqSyncedKey: "tlog:last_slave_sync_seq:" + apc.VdiskID,
	}

	// create redis pool for the metadata
	if err := ss.initMetaRedisPool(configPath); err != nil {
		return nil, err
	}

	if err := ss.start(); err != nil {
		return nil, err
	}

	return ss, nil
}

// start the slave syncer.
// make sure to sync the latest synced sequence with
// what stored in tlogserver
func (ss *slaveSyncer) start() error {
	// get slavesyncer's latest synced sequence
	lastSyncedSeq, err := ss.getLastSyncedSeq()
	if err != nil {
		return fmt.Errorf("getLastSyncedSeq failed:%v", err)
	}

	// get tlog's latest flushed sequence and catch up if needed
	// we can do it by simply replaying all sequence after last synced sequence
	lastSyncedSeq, err = ss.player.ReplayWithCallback(ss.decodeLimiter(lastSyncedSeq),
		ss.setLastSyncedSeq)
	if err != nil && err != decoder.ErrNilLastHash {
		return err
	}

	go ss.run(lastSyncedSeq)

	return nil
}

func (ss *slaveSyncer) run(lastSeqSynced uint64) {
	log.Infof("run slave syncer for vdisk '%v'", ss.vdiskID)

	defer log.Infof("slave syncer for vdisk '%v' exited", ss.vdiskID)

	var waitForSync bool // true if we currently wait for sync event
	var seqToWait uint64 // sequence to wait to be synced

	// closure to mark that we've synced the slave
	finishWaitForSync := func() {
		waitForSync = false
		ss.aggComm.SendResp(nil)
	}

	defer ss.mgr.remove(ss.vdiskID)

	for {
		select {
		case <-ss.ctx.Done():
			// our producer (tlog's vdisk) exited
			// so we are!
			// TODO : fix it! we need to wait for the slave sync to be finished
			return

		case rawAgg := <-ss.aggComm.RecvAgg():
			// raw aggregation []byte
			seq, err := ss.replay(rawAgg, lastSeqSynced)
			if err != nil {
				log.Error(err)
				continue
			}
			lastSeqSynced = seq

			if waitForSync && lastSeqSynced >= seqToWait {
				finishWaitForSync()
			}

		case cmd := <-ss.aggComm.RecvCmd():
			// receive a command
			switch cmd.Type {
			case aggmq.CmdKillMe:
				return

			case aggmq.CmdWaitSlaveSync:
				// wait for slave to be fully synced
				seqToWait = cmd.Seq
				if lastSeqSynced >= seqToWait {
					finishWaitForSync()
				} else {
					waitForSync = true
				}
			}
		}
	}
}

func (ss *slaveSyncer) decodeLimiter(lastSeqSynced uint64) decoder.Limiter {
	startSeq := lastSeqSynced + 1
	if lastSeqSynced == 0 {
		startSeq = 0
	}
	return decoder.NewLimitBySequence(startSeq, 0)
}

// replay a raw aggregation (in []byte) to the ardb slave
func (ss *slaveSyncer) replay(rawAgg aggmq.AggMqMsg, lastSeqSynced uint64) (uint64, error) {
	msg, err := capnp.NewDecoder(bytes.NewReader([]byte(rawAgg))).Decode()
	if err != nil {
		return 0, fmt.Errorf("failed to decode agg:%v", err)
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to read root tlog:%v", err)
	}

	return ss.player.ReplayAggregationWithCallback(&agg, ss.decodeLimiter(lastSeqSynced), ss.setLastSyncedSeq)
}

// set last synced sequence to slave metadata
func (ss *slaveSyncer) setLastSyncedSeq(seq uint64) error {
	rc := ss.metaPool.Get(ss.metaConf.Address, ss.metaConf.Database)
	defer rc.Close()

	_, err := rc.Do("SET", ss.lastSeqSyncedKey, seq)
	return err
}

// get last synced sequence
func (ss *slaveSyncer) getLastSyncedSeq() (uint64, error) {
	rc := ss.metaPool.Get(ss.metaConf.Address, ss.metaConf.Database)
	defer rc.Close()

	seq, err := redis.Uint64(rc.Do("GET", ss.lastSeqSyncedKey))
	if err == nil || err == redis.ErrNil {
		return seq, nil
	}
	return seq, err
}

// initialize redis pool for metadata connection
func (ss *slaveSyncer) initMetaRedisPool(configPath string) error {
	// get meta storage config
	metaStorConf, err := func(vdiskID string) (*config.StorageServerConfig, error) {
		// read config
		conf, err := config.ReadConfig(configPath, config.TlogServer)
		if err != nil {
			return nil, err
		}
		// get storage config for this vdisk
		vdiskConf, ok := conf.Vdisks[ss.vdiskID]
		if !ok {
			return nil, fmt.Errorf("config for vdisk '%v' not found", vdiskID)
		}

		storClust, ok := conf.StorageClusters[vdiskConf.StorageCluster]
		if !ok {
			return nil, fmt.Errorf("no storageCluster for vdisk '%v", vdiskID)
		}
		return storClust.MetadataStorage, nil
	}(ss.vdiskID)

	if err != nil {
		return err
	}

	ss.metaConf = metaStorConf
	ss.metaPool = ardb.NewRedisPool(nil)
	return nil
}
