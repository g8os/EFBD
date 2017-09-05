package statistics

import (
	"context"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// VdiskLogger defines an nbd  statistics logger interface
type VdiskLogger interface {
	LogReadOperation(bytes int64)
	LogWriteOperation(bytes int64)

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
		ctx:                ctx,
		readThroughputKey:  "vdisk.throughput.read@virt." + vdiskID,
		readIOPSKey:        "vdisk.iops.read@virt." + vdiskID,
		writeThroughputKey: "vdisk.throughput.write@virt." + vdiskID,
		writeIOPSKey:       "vdisk.iops.write@virt." + vdiskID,
		tags:               log.MetricTags{},
		readDataCh:         make(chan int64, 8),
		writeDataCh:        make(chan int64, 8),
		configCh:           configCh,
		cancelFunc:         cancel,
		broadcastFunc:      broadcastStatistic,
	}
	go logger.background()
	return logger, nil
}

type vdiskLogger struct {
	ctx                              context.Context
	readThroughputKey, readIOPSKey   string
	writeThroughputKey, writeIOPSKey string
	tags                             log.MetricTags
	readDataCh, writeDataCh          chan int64
	configCh                         <-chan config.VdiskNBDConfig
	cancelFunc                       context.CancelFunc
	broadcastFunc                    broadcastFunc
}

type broadcastFunc func(key string, value float64, tags log.MetricTags)

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

func (vl *vdiskLogger) background() {
	var cfg config.VdiskNBDConfig
	var bytes int64

	var writeAggregator, readAggregator vdiskAggregator

	for {
		select {
		case <-vl.ctx.Done():
			log.Debug("exit vdiskLogger because context is done")
			return

		case cfg = <-vl.configCh:
			if vl.tags[clusterIDKey] != cfg.StorageClusterID {
				vl.tags[clusterIDKey] = cfg.StorageClusterID

				readAggregator.Stop()
				writeAggregator.Stop()
			}

		case bytes = <-vl.readDataCh:
			readAggregator.TrackBytes(bytes)

		case bytes = <-vl.writeDataCh:
			writeAggregator.TrackBytes(bytes)

		// TODO: perhaps ensure that we also collect the last time we write,
		//       so that we only average values where we were actually active

		case st := <-readAggregator.C:
			iops, throughput, duration := readAggregator.Reset(st)
			vl.broadcastFunc(vl.readIOPSKey, float64(iops), vl.tags)
			tv := float64(throughput) / float64(duration.Seconds())
			vl.broadcastFunc(vl.readThroughputKey, tv, vl.tags)

		case st := <-writeAggregator.C:
			iops, throughput, duration := writeAggregator.Reset(st)
			vl.broadcastFunc(vl.writeIOPSKey, float64(iops), vl.tags)
			tv := float64(throughput) / float64(duration.Seconds())
			vl.broadcastFunc(vl.writeThroughputKey, tv, vl.tags)
		}
	}
}

type vdiskAggregator struct {
	C <-chan time.Time

	start            time.Time
	timer            *time.Timer
	iops, throughput int64
}

func (agg *vdiskAggregator) TrackBytes(bytes int64) {
	agg.iops++
	agg.throughput += bytes

	if agg.timer == nil {
		agg.start = time.Now()
		agg.timer = time.NewTimer(vdiskAggregationDuration)
		agg.C = agg.timer.C
	}
}

func (agg *vdiskAggregator) Stop() {
	if agg.timer == nil {
		return
	}

	agg.timer.Stop()
}

func (agg *vdiskAggregator) Reset(st time.Time) (iops, throughput int64, duration time.Duration) {
	iops = agg.iops
	throughput = agg.throughput

	duration = st.Sub(agg.start)
	if duration > vdiskAggregationDuration {
		duration = vdiskAggregationDuration
	}

	agg.timer, agg.C = nil, nil
	agg.iops, agg.throughput = 0, 0
	return
}

const (
	clusterIDKey = "clusterID"
	// TODO: change value
	vdiskAggregationDuration = time.Second * 5
)
