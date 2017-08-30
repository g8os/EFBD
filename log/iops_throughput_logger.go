package log

import (
	"context"
	"sync"
	"time"
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
	ctx           context.Context
	dataMessenger chan<- int64
	cancel        context.CancelFunc
}

// Send sends amount of bytes (read/written) to the log data(bytes) aggrigator
func (l IOPSThroughputLogger) Send(data int64) {
	select {
	case <-l.ctx.Done():
		Debug("Couldn't send statistic, logger is in done state")
	default:
		l.dataMessenger <- data
	}
}

// Stop stops the statistics logging
func (l IOPSThroughputLogger) Stop() {
	l.cancel()
}

// StartIOPSThroughputStatsLoggerRead sets up statistics logger for read IOPS and throughput
func StartIOPSThroughputStatsLoggerRead(vdiskID string, blockSize int64) IOPSThroughputLogger {
	return iopsThroughputStatsLogger(iopsRead, vdiskID, blockSize)
}

// StartIOPSThroughputStatsLoggerWrite sets up statistics logger for write IOPS and throughput
func StartIOPSThroughputStatsLoggerWrite(vdiskID string, blockSize int64) IOPSThroughputLogger {
	return iopsThroughputStatsLogger(iopsWrite, vdiskID, blockSize)
}

// IOPSTroughputStatsLogger sets up statistics logger for IOPS and throughput
func iopsThroughputStatsLogger(direction iopsDirection, vdiskID string, blockSize int64) IOPSThroughputLogger {
	dataMsg := make(chan int64)
	var data int64
	var dataLock sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	if !direction.validate() {
		Error("IOPS and throughput statistics interval logger closed due to invalid keys")
		cancel()
	} else {
		go dataAggregator(ctx, dataMsg, &data, &dataLock)
		go iopsTroughputIntervalLogger(ctx, cancel, vdiskID, blockSize, direction, &dataLock, &data)
	}

	return IOPSThroughputLogger{
		ctx:           ctx,
		cancel:        cancel,
		dataMessenger: dataMsg,
	}
}

// dataAggregator collects data passed to the dataMsg channel
// should be run as goroutine
func dataAggregator(ctx context.Context, dataMsg <-chan int64, data *int64, dataLock *sync.Mutex) {
	for {
		select {
		case val := <-dataMsg:
			dataLock.Lock()
			*data = *data + val
			dataLock.Unlock()
		case <-ctx.Done():
			Debug("IOPS/throughput statistics aggregator closed")
			return
		}
	}
}

// iopsTroughputIntervalLogger will broadcast IOPS and throughput statistics at every interval
// should be run as goroutine
func iopsTroughputIntervalLogger(ctx context.Context, cancel context.CancelFunc, vdiskID string, blockSize int64, direction iopsDirection, dataLock *sync.Mutex, data *int64) {
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
			broadcastFunc(ctx, vdiskID, blockSize, bytesPerSec, iopsKey, tpKey)
		case <-ctx.Done():
			Debug("IOPS/throughput statistics interval logger closed")
			return
		}
	}
}

func broadcastIOPSThroughput(ctx context.Context, vdiskID string, blockSize int64, bytesPerSec float64, iopsKey, tpKey StatisticsKey) {
	// no need to log if 0?
	if bytesPerSec == 0 {
		Debug("statistic value was 0, skipped broadcasting")
		return
	}

	// broadcast iops
	value := bytesPerSec / float64(blockSize)
	BroadcastStatistics(
		vdiskID,
		iopsKey,
		value,
		AggregationAverages,
		nil,
	)

	// broadcast throughput (kB/s)
	value = bytesPerSec / 1024
	BroadcastStatistics(
		vdiskID,
		tpKey,
		value,
		AggregationAverages,
		nil,
	)
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

func (d iopsDirection) iopsKey() StatisticsKey {
	switch d {
	case iopsRead:
		return StatisticsKeyIOPSRead
	case iopsWrite:
		return StatisticsKeyIOPSWrite
	default:
		return StatisticsKeyEnumLength
	}
}

func (d iopsDirection) throughputKey() StatisticsKey {
	switch d {
	case iopsRead:
		return StatisticsKeyTroughputRead
	case iopsWrite:
		return StatisticsKeyTroughputWrite
	default:
		return StatisticsKeyEnumLength
	}
}
