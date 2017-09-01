package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

var (
	// broadcast interval in seconds
	interval = 1 * time.Minute
	// func that does the broadcasting
	//is stubbed during testing
	broadcastFunc = broadcastIOPSThroughput
)

// IOPSThroughputLogger is the API
// used for interacting with the statistics logger
type IOPSThroughputLogger struct {
	vdiskID       string
	ctx           context.Context
	dataMessenger chan<- int64
	cancel        context.CancelFunc
}

// Send sends amount of bytes (read/written) to the log data(bytes) aggrigator
func (l IOPSThroughputLogger) Send(data int64) {
	if l.ctx == nil {
		return
	}

	select {
	case <-l.ctx.Done():
		log.Debugf("Couldn't send statistic, IOPS/throughput logger is in done state for vdisk: %s", l.vdiskID)
	default:
		l.dataMessenger <- data
	}
}

// Stop stops the statistics logging
func (l IOPSThroughputLogger) Stop() {
	if l.ctx == nil {
		return
	}
	log.Infof("Stopping IOPS/Throughput statistics logger for vdisk: %s", l.vdiskID)
	l.cancel()
}

// StartIOPSThroughputRead sets up statistics logger for read IOPS and throughput
func StartIOPSThroughputRead(source config.Source, vdiskID string, blockSize int64) IOPSThroughputLogger {
	return iopsThroughput(iopsRead, source, vdiskID, blockSize)
}

// StartIOPSThroughputWrite sets up statistics logger for write IOPS and throughput
func StartIOPSThroughputWrite(source config.Source, vdiskID string, blockSize int64) IOPSThroughputLogger {
	return iopsThroughput(iopsWrite, source, vdiskID, blockSize)
}

// IOPSTroughput sets up statistics logger for IOPS and throughput
func iopsThroughput(direction iopsDirection, source config.Source, vdiskID string, blockSize int64) IOPSThroughputLogger {
	if !direction.validate() {
		log.Errorf("IOPS and throughput statistics interval logger closed due to invalid keys for vdisk: %s", vdiskID)
		return IOPSThroughputLogger{}
	}

	dataMsg := make(chan int64)
	ctx, cancel := context.WithCancel(context.Background())
	var (
		data          int64
		dataLock      sync.Mutex
		clusterID     string
		clusterIDLock sync.Mutex
	)

	nbdConfigUpdate, err := config.WatchVdiskNBDConfig(ctx, source, vdiskID)
	if err != nil {
		log.Errorf("couldn't get nbd config watcher to run, clusterID will be empty for vdisk: %s", vdiskID)
	} else {
		go clusterIDUpdater(ctx, vdiskID, nbdConfigUpdate, &clusterID, &clusterIDLock)
	}
	go dataAggregator(ctx, vdiskID, dataMsg, &data, &dataLock)
	go iopsTroughputIntervalLogger(ctx, vdiskID, direction, &clusterID, &clusterIDLock, blockSize, &dataLock, &data)

	log.Infof("Starting IOPS/Throughput statistics logger for vdisk: %s", vdiskID)

	return IOPSThroughputLogger{
		vdiskID:       vdiskID,
		ctx:           ctx,
		cancel:        cancel,
		dataMessenger: dataMsg,
	}
}

// dataAggregator collects data passed to the dataMsg channel
// should be run as goroutine
func dataAggregator(ctx context.Context, vdiskID string, dataMsg <-chan int64, data *int64, dataLock *sync.Mutex) {
	for {
		select {
		case val := <-dataMsg:
			dataLock.Lock()
			*data = *data + val
			dataLock.Unlock()
		case <-ctx.Done():
			log.Debugf("IOPS/throughput statistics aggregator closed for vdisk: %s", vdiskID)
			return
		}
	}
}

// iopsTroughputIntervalLogger will broadcast IOPS and throughput statistics at every interval
// should be run as goroutine
func iopsTroughputIntervalLogger(ctx context.Context, vdiskID string, direction iopsDirection, clusterID *string, clusterIDLock *sync.Mutex, blockSize int64, dataLock *sync.Mutex, data *int64) {
	ticker := time.NewTicker(interval)
	iopsKey := direction.iopsKey()
	tpKey := direction.throughputKey()

	for {
		select {
		case <-ticker.C:
			dataLock.Lock()
			bytesPerSec := float64(*data) / interval.Seconds()
			*data = 0
			dataLock.Unlock()
			clusterIDLock.Lock()
			var tags MetricTags
			if *clusterID != "" {
				tags = MetricTags{
					"cluster_id": *clusterID,
				}
			}
			clusterIDLock.Unlock()
			broadcastFunc(vdiskID, blockSize, bytesPerSec, iopsKey, tpKey, tags)
		case <-ctx.Done():
			log.Debugf("IOPS/throughput statistics interval logger closed for vdisk: %s", vdiskID)
			return
		}
	}
}

// broadcastIOPSThroughput actually broadcast statistics based on provided data
func broadcastIOPSThroughput(vdiskID string, blockSize int64, bytesPerSec float64, iopsKey, tpKey Key, tags MetricTags) {
	// no need to log if 0?
	if bytesPerSec == 0 {
		log.Debugf("statistic value was 0, skipped broadcasting for vdisk: %s", vdiskID)
		return
	}

	// broadcast iops
	value := bytesPerSec / float64(blockSize)
	Broadcast(
		vdiskID,
		iopsKey,
		value,
		AggregationAverages,
		tags,
	)

	// broadcast throughput (kB/s)
	value = bytesPerSec / 1024
	Broadcast(
		vdiskID,
		tpKey,
		value,
		AggregationAverages,
		tags,
	)
}

func clusterIDUpdater(ctx context.Context, vdiskID string, nbdConfigUpdate <-chan config.VdiskNBDConfig, clusterID *string, clusterIDLock *sync.Mutex) {
	for {
		select {
		case cfg := <-nbdConfigUpdate:
			clusterIDLock.Lock()
			*clusterID = cfg.StorageClusterID
			clusterIDLock.Unlock()
		case <-ctx.Done():
			log.Debugf("clusterID updater for statistics logger closed for vdisk: %s", vdiskID)
			return
		}
	}
}

// IOPSDirection defines if IOPS logging is for reading or writing
type iopsDirection uint8

// IOPS direction options
const (
	iopsRead = iopsDirection(iota)
	iopsWrite
	iopsDirectionLength
)

func (d iopsDirection) validate() bool {
	if d >= iopsDirectionLength {
		return false
	}
	return true
}

func (d iopsDirection) iopsKey() Key {
	switch d {
	case iopsRead:
		return KeyIOPSRead
	case iopsWrite:
		return KeyIOPSWrite
	default:
		return KeyEnumLength
	}
}

func (d iopsDirection) throughputKey() Key {
	switch d {
	case iopsRead:
		return KeyTroughputRead
	case iopsWrite:
		return KeyTroughputWrite
	default:
		return KeyEnumLength
	}
}
