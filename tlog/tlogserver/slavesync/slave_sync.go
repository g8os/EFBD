package slavesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

// Manager defines slave syncer manager
type Manager struct {
	apMq       *aggmq.MQ
	syncers    map[string]*slaveSyncer
	configInfo zerodisk.ConfigInfo
	mux        sync.Mutex
	ctx        context.Context
}

// NewManager creates new slave syncer manager
func NewManager(ctx context.Context, apMq *aggmq.MQ, configInfo zerodisk.ConfigInfo) *Manager {
	m := &Manager{
		apMq:       apMq,
		configInfo: configInfo,
		syncers:    make(map[string]*slaveSyncer),
		ctx:        ctx,
	}
	return m
}

// Run runs the slave syncer manager
func (m *Manager) Run() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case apr := <-m.apMq.NeedProcessorCh:
			m.handleReq(apr)
		}
	}
}

// handle request
func (m *Manager) handleReq(apr aggmq.AggProcessorReq) {
	m.mux.Lock()
	defer m.mux.Unlock()

	log.Infof("slavesync mgr: create for vdisk: %v", apr.Config.VdiskID)

	// check if the syncer already exist
	if _, ok := m.syncers[apr.Config.VdiskID]; ok {
		m.apMq.NeedProcessorResp <- nil
		return
	}

	// create slave syncer
	ss, err := newSlaveSyncer(m.ctx, m.configInfo, apr.Config, apr.Comm, m)
	if err != nil {
		log.Errorf("slavesync mgr: failed to create syncer for vdisk: %v, err: %v", apr.Config.VdiskID, err)
		m.apMq.NeedProcessorResp <- err
		return
	}
	m.syncers[apr.Config.VdiskID] = ss

	// send response
	m.apMq.NeedProcessorResp <- nil
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
	configInfo       zerodisk.ConfigInfo
	apc              aggmq.AggProcessorConfig

	// we put these three  variables here
	// to make it survive in case of restart
	waitForSync   bool   // true if we currently wait for sync event
	seqToWait     uint64 // sequence to wait to be synced
	lastSyncedSeq uint64 // last synced sequence
}

// newSlaveSyncer creates a new slave syncer
func newSlaveSyncer(ctx context.Context, configInfo zerodisk.ConfigInfo, apc aggmq.AggProcessorConfig,
	aggComm *aggmq.AggComm, mgr *Manager) (*slaveSyncer, error) {

	ss := &slaveSyncer{
		vdiskID:          apc.VdiskID,
		ctx:              ctx,
		aggComm:          aggComm,
		mgr:              mgr,
		configInfo:       configInfo,
		apc:              apc,
		lastSeqSyncedKey: "tlog:last_slave_sync_seq:" + apc.VdiskID,
	}
	return ss, ss.init()
}

func (ss *slaveSyncer) init() error {
	// tlog replay player
	player, err := player.NewPlayer(ss.ctx, ss.configInfo, nil, ss.apc.VdiskID, ss.apc.PrivKey,
		ss.apc.HexNonce, ss.apc.K, ss.apc.M)
	if err != nil {
		return err
	}

	ss.player = player

	// create redis pool for the metadata
	if err := ss.initMetaRedisPool(); err != nil {
		return err
	}

	if err := ss.start(); err != nil {
		return err
	}

	return nil
}

func (ss *slaveSyncer) Close() {
	ss.player.Close()
}

func (ss *slaveSyncer) restart() {
	if err := ss.init(); err != nil {
		log.Errorf("restarting slave syncer for `%v` failed: %v", ss.vdiskID, err)
		return
	}
}

// start the slave syncer.
// make sure to sync the latest synced sequence with
// what stored in tlogserver
func (ss *slaveSyncer) start() error {
	var err error
	// get slavesyncer's latest synced sequence
	ss.lastSyncedSeq, err = ss.getLastSyncedSeq()
	if err != nil {
		return fmt.Errorf("getLastSyncedSeq failed:%v", err)
	}

	// get tlog's latest flushed sequence and catch up if needed
	// we can do it by simply replaying all sequence after last synced sequence
	ss.lastSyncedSeq, err = ss.player.ReplayWithCallback(ss.decodeLimiter(ss.lastSyncedSeq),
		ss.setLastSyncedSeq)
	if err != nil && err != decoder.ErrNilLastHash {
		return err
	}

	go ss.run()

	return nil
}

func (ss *slaveSyncer) run() {
	log.Infof("slave syncer (%v): started", ss.vdiskID)

	defer log.Infof("slave syncer (%v): exited", ss.vdiskID)
	// true if we want to restart this syncer
	// we simply restart in case of error, it is simpler yet more robust
	var needRestart bool

	// true if tlog vdisk already exited
	// we also need to exit :)
	var vdiskExited bool

	// closure to mark that we've synced the slave
	finishWaitForSync := func() {
		if ss.waitForSync && ss.lastSyncedSeq >= ss.seqToWait {
			ss.waitForSync = false
			ss.aggComm.SendResp(nil)
		}
	}

	defer func() {
		if needRestart && !vdiskExited {
			ss.restart()
		} else {
			ss.mgr.remove(ss.vdiskID)
		}
	}()

	for {
		select {
		case <-ss.ctx.Done():
			// our producer (tlog's vdisk) exited
			// so we are!
			// but we need to wait until we sync all of it
			if len(ss.aggComm.RecvAgg()) == 0 {
				return
			}
			vdiskExited = true

		case rawAgg, more := <-ss.aggComm.RecvAgg():
			if !more {
				return
			}
			// raw aggregation []byte
			seq, err := ss.replay(rawAgg, ss.lastSyncedSeq)
			if err != nil {
				needRestart = true
				return
			}
			ss.lastSyncedSeq = seq

			finishWaitForSync()

			if vdiskExited {
				return
			}

		case cmd := <-ss.aggComm.RecvCmd():
			// receive a command
			switch cmd.Type {
			case aggmq.CmdKillMe:
				log.Infof("slave syncer (%v): killed", ss.vdiskID)

				ss.Close()
				ss.mgr.remove(ss.vdiskID)

				return
			case aggmq.CmdRestartSlaveSyncer:
				log.Infof("slave syncer (%v): restarted", ss.vdiskID)

				ss.Close()
				needRestart = true

				return

			case aggmq.CmdWaitSlaveSync:
				// wait for slave to be fully synced
				ss.seqToWait = cmd.Seq
				ss.waitForSync = true

				finishWaitForSync()
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
func (ss *slaveSyncer) initMetaRedisPool() error {
	// get meta storage config
	_, conf, err := zerodisk.ReadNBDConfig(ss.vdiskID, ss.configInfo)
	if err != nil {
		return err
	}

	if conf.StorageCluster.MetadataStorage == nil {
		return errors.New(
			"no meta storage cluster configured for vdisk " + ss.vdiskID)
	}

	if err != nil {
		return err
	}

	ss.metaConf = conf.StorageCluster.MetadataStorage
	ss.metaPool = ardb.NewRedisPool(nil)
	return nil
}
