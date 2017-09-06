package statistics

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// ====
// [TODO] Answer following questions:
//
//   Are we OK that due to irregular activity that it won't be uncommon
// to have lower average aggregation statistics (iops/throughput) values.
// Meaning that we might broadcast a low IOPS/throughput value,
// not because the vdisk is performing bad, but because we have an interval period,
// where we have one operation at the start, and another one closer to the end of the interval,
// with a big gap in between.
//    => I'm not sure if there is a good solution as long as we wish to keep average values,
//       however this problem could be partly avoided by logging every second,
//       as we'll have much more accurate values due to the fact that the gaps
//       will be very tiny if they exist at all, hence not invalidating the data.
//
// ====

// VdiskLogger defines an nbd  statistics logger interface
type VdiskLogger interface {
	// LogReadOperation logs a read operation,
	// using it to keep track of the read IOPS and read throughput (in KiB/s).
	LogReadOperation(bytes int64)
	// LogWriteOperation logs a write operation,
	// using it to keep track of the write IOPS and write throughput (in KiB/s).
	LogWriteOperation(bytes int64)

	// Close all open resources and
	// stop all background goroutines linked to this vdiskLogger.
	Close() error
}

// NewVdiskLogger creates a new VdiskLogger which
// tracks the read and write operations of a vdisk for statistics purposes.
func NewVdiskLogger(ctx context.Context, configSource config.Source, vdiskID string) (VdiskLogger, error) {
	ctx, cancel := context.WithCancel(ctx)

	configCh, err := config.WatchVdiskNBDConfig(ctx, configSource, vdiskID)
	if err != nil {
		cancel()
		return nil, err
	}

	logger := &vdiskLogger{
		// context-related values
		ctx:        ctx,
		cancelFunc: cancel,

		// pre-computed statistics keys
		readThroughputKey:  "vdisk.throughput.read@virt." + vdiskID,
		readIOPSKey:        "vdisk.iops.read@virt." + vdiskID,
		writeThroughputKey: "vdisk.throughput.write@virt." + vdiskID,
		writeIOPSKey:       "vdisk.iops.write@virt." + vdiskID,

		// empty tags for now
		tags: log.MetricTags{},
		// configCh to keep track of incoming config changes,
		// and used as the input for the metric tags of this logger
		configCh: configCh,

		// incoming bytes (data) channel
		readDataCh:  make(chan int64, 8),
		writeDataCh: make(chan int64, 8),

		// delegator to do the actual broadcasting work
		// for each (early) finished aggregation interval
		broadcastFunc: broadcastStatistic,
	}
	go logger.background()
	return logger, nil
}

// vdiskLogger is used to gather r/w iops/throughput statistics for a given vdisk.
type vdiskLogger struct {
	// the context used for all background purposes of this vdiskLogger
	ctx context.Context
	// cancel the ctx property,
	// stopping the background thread of this logger,
	// as well as the running vdisk's cluster's config watcher
	cancelFunc context.CancelFunc

	// precomputed keys for this vdisk,
	// used to broadcast the statistics linked to these keys
	readThroughputKey, readIOPSKey   string
	writeThroughputKey, writeIOPSKey string

	// the tags contain the clusterID information
	tags log.MetricTags
	// configCh used to ensure this logger is using
	// at all times the most up to date config for its metric tags.
	// this is important as switching to a different cluster,
	// might explain a sudden change in the incoming stats values
	configCh <-chan config.VdiskNBDConfig

	// buffered bytes channels for read and write purposes,
	// buffered to ensure that a vdisk is never blocked on collecting
	// aggregated stats input
	readDataCh, writeDataCh chan int64

	// for all purposes other than testing,
	// this func is equal to `broadcastStatistic`
	broadcastFunc broadcastFunc

	// aggregators used for write and read operations
	writeAggregator, readAggregator vdiskAggregator
}

// broadcastFunc is an internal function definition,
// such that we can overwrite the broadcastFunc logic for a given vdiskLogger,
// for testing purposes.
type broadcastFunc func(key string, value float64, tags log.MetricTags)

// broadcastStatistics is the actual broadcast function used for production,
// using the zero-os/0-log lib wrapped in our log module,
// in order to do zero-os stats logging (level 10) over STDERR.
func broadcastStatistic(key string, value float64, tags log.MetricTags) {
	log.BroadcastStatistics(key, value, log.AggregationAverages, tags)
}

// LogReadOperation implements VdiskLogger.LogReadOperation
func (vl *vdiskLogger) LogReadOperation(bytes int64) {
	vl.readDataCh <- bytes
}

// LogWriteOperation implements VdiskLogger.LogWriteOperation
func (vl *vdiskLogger) LogWriteOperation(bytes int64) {
	vl.writeDataCh <- bytes
}

// Close implements VdiskLogger.Close
func (vl *vdiskLogger) Close() error {
	vl.cancelFunc()
	return nil
}

// the background worker for a vdisk logger,
// keeping track of the vdisk's cluster config, buffering of incoming r/w bytes,
// and broadcasting values at planned and early intervals.
func (vl *vdiskLogger) background() {
	var cfg config.VdiskNBDConfig
	var bytes int64

	for {
		select {
		// context is finished, log for one last time and exit
		case <-vl.ctx.Done():
			log.Debug("logging last minute vdisks statistics")
			vl.broadcastReadStatistics()
			vl.broadcastWriteStatistics()

			log.Debug("exit vdiskLogger because context is done")
			return

		// one of our stats timers is finished
		case <-vl.readAggregator.C:
			vl.broadcastReadStatistics()
		case <-vl.writeAggregator.C:
			vl.broadcastWriteStatistics()

		// config has updated, check if our tags change
		case cfg = <-vl.configCh:
			if vl.tags[clusterIDKey] != cfg.StorageClusterID {
				vl.tags[clusterIDKey] = cfg.StorageClusterID

				vl.readAggregator.Stop()
				vl.writeAggregator.Stop()
			}

		// incoming data
		case bytes = <-vl.readDataCh:
			vl.readAggregator.TrackBytes(bytes)
		case bytes = <-vl.writeDataCh:
			vl.writeAggregator.TrackBytes(bytes)
		}
	}
}

// internal func to reset the read aggregator
// and broadcast its aggregated iops/throughput values.
func (vl *vdiskLogger) broadcastReadStatistics() {
	iops, throughput := vl.readAggregator.Reset()
	if iops == 0 {
		return
	}

	vl.broadcastFunc(vl.readIOPSKey, iops, vl.tags)
	vl.broadcastFunc(vl.readThroughputKey, throughput, vl.tags)
}

// internal func to reset the write aggregator
// and broadcast its aggregated iops/throughput values.
func (vl *vdiskLogger) broadcastWriteStatistics() {
	iops, throughput := vl.writeAggregator.Reset()
	if iops == 0 {
		return
	}

	vl.broadcastFunc(vl.writeIOPSKey, iops, vl.tags)
	vl.broadcastFunc(vl.writeThroughputKey, throughput, vl.tags)
}

// vdiskAggregator is used to aggregate values for a given r/w direction.
type vdiskAggregator struct {
	// public timer channel,
	// only ever set while this aggregator is active
	C <-chan time.Time

	// time values
	start, end time.Time
	timer      *time.Timer

	// total values
	iops       int64
	throughput big.Float
}

// Track the operation and the bytes that go with it.
// It also starts an interval (of an active period) in case one wasn't started yet.
// Each tracking also moves the end time up, as it marks the positive limit of this interval.
func (agg *vdiskAggregator) TrackBytes(bytes int64) {
	agg.iops++
	agg.throughput.Add(&agg.throughput, big.NewFloat(0).SetInt64(bytes))
	agg.end = time.Now()

	if agg.timer == nil {
		agg.start = time.Now()
		agg.timer = time.NewTimer(vdiskAggregationDuration)
		agg.C = agg.timer.C
	}
}

// Stop this vdisk aggregator,
// this will result in the Reset function being called a bit later.
// A manual stop can be useful in case we need to stop before the planned interval duration.
func (agg *vdiskAggregator) Stop() {
	if agg.timer == nil {
		return
	}

	agg.timer.Stop()
}

// Reset returns the aggregate values as an average over the active duration,
// and reset the aggregator's internal values.
func (agg *vdiskAggregator) Reset() (iops, throughput float64) {
	if agg.timer == nil {
		return
	}

	// compute averages based on the aggregated values
	iops, throughput = agg.computeAverages()

	if iops < 1.0 {
		fmt.Fprintf(os.Stderr,
			"totalIOPS = %d ; totalThroughput = %v ; start = %v ; end = %v ; durInSecs = %v\n",
			agg.iops, agg.throughput, agg.start, agg.end, agg.end.Sub(agg.start).Seconds())
	}

	// reset all values
	agg.timer, agg.C = nil, nil
	agg.iops = 0
	agg.throughput.SetFloat64(0)

	// return compute results
	return
}

func (agg *vdiskAggregator) computeAverages() (iops, throughput float64) {
	// if start == end, it means we only have received one operation,
	if agg.start == agg.end {
		iops = float64(agg.iops)
		throughput, _ = agg.throughput.Float64()
		return
	}
	// received multiple operations (hopefully more than 2)

	// compute interval durations
	dur := agg.end.Sub(agg.start)
	dursecs := big.NewFloat(dur.Seconds())

	// compute IOPS
	bigIOPS := big.NewFloat(0).SetInt64(agg.iops)
	iops, _ = bigIOPS.Quo(bigIOPS, dursecs).Float64()

	// compute throughput
	agg.throughput.Quo(&agg.throughput, vdiskThroughputScalar)
	agg.throughput.Quo(&agg.throughput, dursecs)
	throughput, _ = agg.throughput.Float64()

	// return average operations
	return
}

// MaxVdiskAggregationDuration defines the maximum aggregation duration
// used for vdisk operation statistics.
const MaxVdiskAggregationDuration = time.Second

const (
	clusterIDKey = "clusterID"
)

var (
	vdiskAggregationDuration = MaxVdiskAggregationDuration
	vdiskThroughputScalar    = big.NewFloat(1024)
)
