package statistics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-log"
)

var (
	broadcastChan chan broadcastData
)

func TestIOPSThroughputLogger(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)
	blockSize := int64(4096)
	vdiskID := "testVdisk"
	clusterID := "testCluster"
	source := config.NewStubSource()
	source.SetPrimaryStorageCluster(vdiskID, clusterID, &config.StorageClusterConfig{
		DataStorage: []config.StorageServerConfig{
			config.StorageServerConfig{
				Address:  "localhost:123",
				Database: 1,
			},
		},
	})

	// test read stats logger
	// nil source should only log an error
	readLogger := StartIOPSThroughputRead(nil, vdiskID, blockSize).(IOPSThroughputLogger)

	// send one block and check if data to be broadcasted matches up
	readLogger.Send(blockSize)

	bcData := <-broadcastChan
	assert.Equal(float64(blockSize), bcData.datbytesPerSec*interval.Seconds())
	assert.Equal(vdiskID, bcData.vdiskID)
	assert.Equal(blockSize, bcData.blockSize)
	assert.Equal(log.StatisticsKeyIOPSRead, bcData.iopsKey)
	assert.Equal(log.StatisticsKeyTroughputRead, bcData.tpKey)
	// no need for this casting in go >= 1.9
	tags := zerolog.MetricTags(bcData.tags)
	assert.Equal("", tags.String())

	// close and check if context is closed
	readLogger.Stop()

	readLoggerClosed := func() bool {
		select {
		case <-readLogger.ctx.Done():
			return true
		default:
			return false
		}
	}()
	assert.True(readLoggerClosed, "readlogger's context should be in state: Done")

	// Send data while closed
	readLogger.Send(blockSize)
	time.Sleep(interval * 2)
	readLoggerReceivedOnClose := func() bool {
		select {
		case <-broadcastChan:
			return true
		default:
			return false
		}
	}()
	assert.False(readLoggerReceivedOnClose, "should not broadcast when logger is closed")

	// test write stats logger
	writeLogger := StartIOPSThroughputWrite(source, vdiskID, blockSize)

	// send one block and check if data to be broadcasted matches up
	writeLogger.Send(blockSize)
	bcData = <-broadcastChan
	assert.Equal(float64(blockSize), bcData.datbytesPerSec*interval.Seconds())
	assert.Equal(vdiskID, bcData.vdiskID)
	assert.Equal(blockSize, bcData.blockSize)
	assert.Equal(log.StatisticsKeyIOPSWrite, bcData.iopsKey)
	assert.Equal(log.StatisticsKeyTroughputWrite, bcData.tpKey)
	// no need for this casting in go >= 1.9
	tags = zerolog.MetricTags(bcData.tags)
	assert.Equal("cluster_id="+clusterID, tags.String())

	// stop logger as to not interfere with upcoming tests
	writeLogger.Stop()

	// test invalid direction
	faultyLogger := iopsThroughput(255, nil, vdiskID, blockSize)
	defer faultyLogger.Stop()
	faultyLogger.Send(blockSize)
	time.Sleep(interval * 2)
	faultyLoggerReceivedWhileClosed := func() bool {
		select {
		case <-broadcastChan:
			return true
		default:
			return false
		}
	}()
	assert.False(faultyLoggerReceivedWhileClosed, "faultyLogger's context should be in state: Done")
}

func broadcastIOPSThroughputTest(vdiskID string, blockSize int64, bytesPerSec float64, iopsKey, tpKey log.StatisticsKey, tags log.MetricTags) {
	broadcastChan <- broadcastData{
		vdiskID,
		blockSize,
		bytesPerSec,
		iopsKey,
		tpKey,
		tags,
	}
}

type broadcastData struct {
	vdiskID        string
	blockSize      int64
	datbytesPerSec float64
	iopsKey, tpKey log.StatisticsKey
	tags           log.MetricTags
}

func init() {
	interval = 10 * time.Millisecond
	broadcastChan = make(chan broadcastData)
	broadcastFunc = broadcastIOPSThroughputTest
}
