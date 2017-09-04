package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// Logger defines an nbd statistics logger interface
type Logger interface {
	Send(int64)
	Stop()
}

var (
	// broadcast interval in seconds
	interval = 5 * time.Minute
	// func that does the broadcasting
	//is stubbed during testing
	broadcastFunc = broadcastIOPSThroughput
)

// StartIOPSThroughputRead sets up statistics logger for read IOPS and throughput
func StartIOPSThroughputRead(source config.Source, vdiskID string, blockSize int64) Logger {
	return iopsThroughput(iopsRead, source, vdiskID, blockSize)
}

// StartIOPSThroughputWrite sets up statistics logger for write IOPS and throughput
func StartIOPSThroughputWrite(source config.Source, vdiskID string, blockSize int64) Logger {
	return iopsThroughput(iopsWrite, source, vdiskID, blockSize)
}

// IOPSTroughput sets up statistics logger for IOPS and throughput
func iopsThroughput(direction iopsDirection, source config.Source, vdiskID string, blockSize int64) Logger {
	ctx, cancel := context.WithCancel(context.Background())

	if !direction.validate() {
		log.Errorf("IOPS and throughput statistics interval logger closed due to invalid keys for vdisk: %s", vdiskID)
		cancel()
		return IOPSThroughputLogger{
			ctx: ctx,
		}
	}

	dataMsg := make(chan int64)
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
			var tags log.MetricTags
			if *clusterID != "" {
				tags = log.MetricTags{
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
func broadcastIOPSThroughput(vdiskID string, blockSize int64, bytesPerSec float64, iopsKey, tpKey log.StatisticsKey, tags log.MetricTags) {
	// no need to log if 0?
	if bytesPerSec == 0 {
		log.Debugf("statistic value was 0, skipped broadcasting for vdisk: %s", vdiskID)
		return
	}

	// broadcast iops
	value := bytesPerSec / float64(blockSize)
	log.BroadcastStatistics(
		vdiskID,
		iopsKey,
		value,
		log.AggregationAverages,
		tags,
	)

	// broadcast throughput (kB/s)
	value = bytesPerSec / 1024
	log.BroadcastStatistics(
		vdiskID,
		tpKey,
		value,
		log.AggregationAverages,
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

func (d *iopsDirection) iopsKey() log.StatisticsKey {
	switch *d {
	case iopsRead:
		return log.StatisticsKeyIOPSRead
	case iopsWrite:
		return log.StatisticsKeyIOPSWrite
	default:
		return log.StatisticsKeyEnumLength
	}
}

func (d *iopsDirection) throughputKey() log.StatisticsKey {
	switch *d {
	case iopsRead:
		return log.StatisticsKeyTroughputRead
	case iopsWrite:
		return log.StatisticsKeyTroughputWrite
	default:
		return log.StatisticsKeyEnumLength
	}
}

// DummyLogger defines a dummy logger
// Can be used for testing
type DummyLogger struct{}

// Send is a dummy send method
// implements the Logger interface
func (dl DummyLogger) Send(i int64) {}

// Stop is a dummy stop method
// implements the Logger interface
func (dl DummyLogger) Stop() {}

// IOPSThroughputLogger defines a logger specific for IOPS and Throughput statistics
type IOPSThroughputLogger struct {
	vdiskID       string
	ctx           context.Context
	dataMessenger chan<- int64
	cancel        context.CancelFunc
}

// Send sends amount of bytes (read/written) to the log data(bytes) aggrigator
// implements the Logger interface
func (l IOPSThroughputLogger) Send(data int64) {
	select {
	case <-l.ctx.Done():
		log.Debugf("Couldn't send statistic, IOPS/throughput logger is in done state for vdisk: %s", l.vdiskID)
	default:
		l.dataMessenger <- data
	}
}

// Stop stops the statistics logging
// implements the Logger interface
func (l IOPSThroughputLogger) Stop() {
	select {
	case <-l.ctx.Done():
		return
	default:
		log.Infof("Stopping IOPS/Throughput statistics logger for vdisk: %s", l.vdiskID)
		l.cancel()
	}
}
