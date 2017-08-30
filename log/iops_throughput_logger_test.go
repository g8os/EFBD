package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	broadcastChan chan broadcastData
)

func TestIOPSThroughputLogger(t *testing.T) {
	SetLevel(DebugLevel)
	assert := assert.New(t)
	blockSize := int64(4096)
	vdiskID := "testVdisk"

	// test read stats logger
	readLogger := StartIOPSThroughputStatsLoggerRead(vdiskID, blockSize)

	// send one block and check if data to be broadcasted matches
	readLogger.Send(blockSize)

	bcData := <-broadcastChan
	assert.Equal(float64(blockSize), bcData.datbytesPerSec*interval.Seconds())
	assert.Equal(vdiskID, bcData.vdiskID)
	assert.Equal(blockSize, bcData.blockSize)
	assert.Equal(StatisticsKeyIOPSRead, bcData.iopsKey)
	assert.Equal(StatisticsKeyTroughputRead, bcData.tpKey)

	// close and check if context is closed
	readLogger.Stop()

	readLoggerClosed := func() bool {
		select {
		case <-bcData.ctx.Done():
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
	writeLogger := StartIOPSThroughputStatsLoggerWrite(vdiskID, blockSize)

	// send one block and check if data to be broadcasted matches
	writeLogger.Send(blockSize)
	bcData = <-broadcastChan
	assert.Equal(float64(blockSize), bcData.datbytesPerSec*interval.Seconds())
	assert.Equal(vdiskID, bcData.vdiskID)
	assert.Equal(blockSize, bcData.blockSize)
	assert.Equal(StatisticsKeyIOPSWrite, bcData.iopsKey)
	assert.Equal(StatisticsKeyTroughputWrite, bcData.tpKey)

	// stop logger as to not interfere with upcoming tests
	writeLogger.Stop()

	// test invalid direction
	faultyLogger := iopsThroughputStatsLogger(255, vdiskID, blockSize)
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

type broadcastData struct {
	ctx            context.Context
	vdiskID        string
	blockSize      int64
	datbytesPerSec float64
	iopsKey, tpKey StatisticsKey
}

func broadcastIOPSThroughputTest(ctx context.Context, vdiskID string, blockSize int64, bytesPerSec float64, iopsKey, tpKey StatisticsKey) {
	broadcastChan <- broadcastData{
		ctx,
		vdiskID,
		blockSize,
		bytesPerSec,
		iopsKey,
		tpKey,
	}
}

func init() {
	interval = 10 * time.Millisecond
	broadcastChan = make(chan broadcastData)
	broadcastFunc = broadcastIOPSThroughputTest
}
