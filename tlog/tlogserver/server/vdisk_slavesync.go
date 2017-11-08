package server

import (
	"context"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

const (
	// the amount of time we want to wait for slave to sync to master
	waitSlaveSyncTimeout = 4 * time.Minute
)

// watch slave related config
func (vd *vdisk) watchSlaveConfig() error {
	if vd.slaveSyncMgr == nil {
		return nil
	}
	ctx, cancelFunc := context.WithCancel(vd.ctx)

	// watch vdisk nbd config
	nbdConfCh, err := config.WatchVdiskNBDConfig(ctx, vd.configSource, vd.id)
	if err != nil {
		return err
	}

	var nbdConf config.VdiskNBDConfig
	select {
	case nbdConf = <-nbdConfCh:
	case <-ctx.Done():
		return nil
	}
	slaveClusterID := nbdConf.SlaveStorageClusterID

	// watch for slave storage cluster config
	slaveClusterConfCh := make(chan config.StorageClusterConfig)
	watchSlaveClusterConf := func(ctx context.Context) {
		if slaveClusterID == "" {
			return
		}
		confCh, err := config.WatchStorageClusterConfig(ctx, vd.configSource, slaveClusterID)
		if err != nil {
			cancelFunc()
			return
		}
		select {
		case conf := <-confCh:
			slaveClusterConfCh <- conf
		case <-ctx.Done():
		}
	}

	go func() {
		defer cancelFunc()
		for {
			// watch for slave cluster change
			// we currently re-watch on each iteration
			slaveClusterWatchCtx, slaveClusterWatchCancel := context.WithCancel(ctx)
			watchSlaveClusterConf(slaveClusterWatchCtx)

			select {
			case <-ctx.Done():
				slaveClusterWatchCancel()
				return
			case nbdConf = <-nbdConfCh:
				// slave cluster ID has been modified
				if slaveClusterID != nbdConf.SlaveStorageClusterID {
					slaveClusterID = nbdConf.SlaveStorageClusterID
					if err := vd.manageSlaveSync(); err != nil {
						log.Errorf("manageSlaveSync failed for vdisk `%v`  err: %v", vd.id, err)
					}
				}
			case <-slaveClusterConfCh:
				// slave cluster has been modified
				if err := vd.manageSlaveSync(); err != nil {
					log.Errorf("manageSlaveSync failed for vdisk `%v`  err: %v", vd.id, err)
				}
			}
			slaveClusterWatchCancel()
		}
	}()
	return nil
}

// manage slave syncer lifecycle
func (vd *vdisk) manageSlaveSync() error {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	// it means the slave syncer doesn't activated globally
	if vd.slaveSyncMgr == nil {
		return nil
	}

	// check if we have slave syncer feature activated
	// for this vdisk
	withSlaveSync, err := vd.withSlaveSync()
	if err != nil {
		return err
	}

	if !withSlaveSync {
		if vd.slaveSyncer != nil {
			// stop slave syncer if we previously have it
			vd.slaveSyncer.Stop()
			vd.slaveSyncer = nil
		}
		return nil
	}

	if vd.slaveSyncer != nil {
		log.Infof("Restart slave syncer for vdisk: %v", vd.id)
		// we already have it, restart it in case we have some config update
		// TODO : think again whether we need to restart it
		vd.slaveSyncer.Restart()
		return nil
	}

	log.Infof("Activate slave syncer for vdisk: %v", vd.id)
	ss, err := vd.slaveSyncMgr.Create(vd.id)
	if err != nil {
		return err
	}

	vd.slaveSyncer = ss

	return nil
}

// send wait slave sync command to the flusher
// we do it in blocking way
func (vd *vdisk) waitSlaveSync() error {
	// make sure it has syncer
	if vd.slaveSyncer == nil {
		log.Error("waitSlaveSync command received on vdisk with no slave syncer")
		return nil
	}
	log.Infof("waitSlaveSync for vdisk: %v", vd.id)

	// send the command
	cmd := vdiskFlusherCmd{
		cmdType: vdiskCmdWaitSlaveSync,
		respCh:  make(chan error),
	}
	vd.flusherCmdChan <- cmd

	// wait and return the response
	err := <-cmd.respCh
	if err == nil {
		// we've successfully synced the slave
		// it means the slave is going to be used by nbdserver as it's master
		// so we disable it and kill the slave syncer
		vd.destroySlaveSync()
	}
	log.Debugf("waitSlaveSync for vdisk %v finished with err:%v", vd.id, err)

	return err
}

func (vd *vdisk) doWaitSlaveSync(respCh chan error, lastSeqFlushed uint64) {
	if vd.slaveSyncer == nil {
		respCh <- nil
		return
	}
	respCh <- vd.slaveSyncer.WaitSync(lastSeqFlushed, waitSlaveSyncTimeout)
}

// send raw aggregation to slave syncer
func (vd *vdisk) sendAggToSlaveSync(rawAgg []byte) {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	if vd.slaveSyncer == nil {
		return
	}
	vd.slaveSyncer.SendAgg(rawAgg)
}

// destroy slave syncer
func (vd *vdisk) destroySlaveSync() {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	if vd.slaveSyncer == nil {
		return
	}
	vd.slaveSyncer.Stop()
	vd.slaveSyncer = nil
}

// withSlaveSync returns true if this vdisk is configured with slave syncer.
// which happens if nbd config has SlaveStorageClusterID
func (vd *vdisk) withSlaveSync() (bool, error) {
	nbdConf, err := config.ReadVdiskNBDConfig(vd.configSource, vd.id)
	if err != nil {
		return false, err
	}
	return nbdConf.SlaveStorageClusterID != "", nil
}
