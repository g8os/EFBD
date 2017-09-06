package statistics

import (
	"context"
	"math/big"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

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
	var start, end time.Time
	var duration time.Duration

	// ticker which helps us aggregate values on regular intervals,
	// as to monitor the activity of a vdisk.
	ticker := time.NewTicker(vdiskAggregationDuration)
	defer ticker.Stop()

	start = time.Now()
	for {
		select {
		// context is finished, log for one last time and exit
		case <-vl.ctx.Done():
			log.Debug("logging last minute vdisks statistics")
			end = time.Now()
			duration = end.Sub(start)
			vl.broadcastReadStatistics(duration)
			vl.broadcastWriteStatistics(duration)

			log.Debug("exit vdiskLogger because context is done")
			return

		// interval timer ticks,
		// compute interval duration,
		// and reset start timer
		case end = <-ticker.C:
			duration = end.Sub(start)
			start = end

			vl.broadcastReadStatistics(duration)
			vl.broadcastWriteStatistics(duration)

		// config has updated, check if our tags change
		case cfg = <-vl.configCh:
			vl.tags[clusterKey] = cfg.StorageClusterID
			if cfg.TemplateStorageClusterID == "" {
				delete(vl.tags, templateClusterKey)
			} else {
				vl.tags[templateClusterKey] = cfg.TemplateStorageClusterID
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
func (vl *vdiskLogger) broadcastReadStatistics(duration time.Duration) {
	iops, throughput := vl.readAggregator.Reset(duration)
	vl.broadcastFunc(vl.readIOPSKey, iops, vl.tags)
	vl.broadcastFunc(vl.readThroughputKey, throughput, vl.tags)
}

// internal func to reset the write aggregator
// and broadcast its aggregated iops/throughput values.
func (vl *vdiskLogger) broadcastWriteStatistics(duration time.Duration) {
	iops, throughput := vl.writeAggregator.Reset(duration)
	vl.broadcastFunc(vl.writeIOPSKey, iops, vl.tags)
	vl.broadcastFunc(vl.writeThroughputKey, throughput, vl.tags)
}

// vdiskAggregator is used to aggregate values for a given r/w direction.
type vdiskAggregator struct {
	// time values
	start time.Time

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
}

// Reset returns the aggregate values as an average over the active duration,
// and reset the aggregator's internal values.
func (agg *vdiskAggregator) Reset(duration time.Duration) (iops, throughput float64) {
	if agg.iops == 0 {
		return
	}

	// compute averages based on the aggregated values
	iops, throughput = agg.computeAverages(duration)

	// reset all values
	agg.iops = 0
	agg.throughput.SetFloat64(0)
	agg.start = time.Now()

	// return compute results
	return
}

func (agg *vdiskAggregator) computeAverages(duration time.Duration) (iops, throughput float64) {
	if duration < minVdiskAggregationDuration {
		return
	}
	dursecs := big.NewFloat(duration.Seconds())

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

const (
	// MaxVdiskAggregationDuration defines the maximum aggregation duration
	// used for vdisk operation (average) statistics.
	MaxVdiskAggregationDuration = time.Second * 30
	// MinVdiskAggregationDuration defines the minimum aggregation duration
	// used for vdisk operation (average) statistics.
	MinVdiskAggregationDuration = time.Second
)

const (
	clusterKey         = "cluster"
	templateClusterKey = "templateCluster"
)

var (
	vdiskAggregationDuration    = MaxVdiskAggregationDuration
	minVdiskAggregationDuration = MinVdiskAggregationDuration
	vdiskThroughputScalar       = big.NewFloat(1024)
)
