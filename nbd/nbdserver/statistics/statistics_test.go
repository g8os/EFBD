package statistics

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVdiskAggregator(t *testing.T) {
	assert := assert.New(t)

	var va vdiskAggregator

	// nil aggregator is zero
	iops, throughput := va.Reset(0)
	assert.Zero(iops)
	assert.Zero(throughput)

	// because we average on one second
	// and we do not want to start to predict
	// we cap the duration at 1 second, if it's lower than that

	va.TrackBytes(2048)
	iops, throughput = va.Reset(0)
	assert.Equal(1.0, iops)
	assert.Equal(2.0, throughput)

	va.TrackBytes(2048)
	iops, throughput = va.Reset(time.Millisecond * 100)
	assert.Equal(1.0, iops)
	assert.Equal(2.0, throughput)

	// let's test some different possibilities

	va.TrackBytes(2048)
	iops, throughput = va.Reset(time.Second)
	assert.Equal(1.0, iops)
	assert.Equal(2.0, throughput)

	va.TrackBytes(4096)
	iops, throughput = va.Reset(time.Second)
	assert.Equal(1.0, iops)
	assert.Equal(4.0, throughput)

	va.TrackBytes(1024)
	va.TrackBytes(1024)
	va.TrackBytes(2048)
	iops, throughput = va.Reset(time.Second)
	assert.Equal(3.0, iops)
	assert.Equal(4.0, throughput)

	// the througput total count should never overflow

	va.TrackBytes(math.MaxInt64)
	va.TrackBytes(math.MaxInt64)
	iops, throughput = va.Reset(time.Second)
	assert.Equal(2.0, iops)
	assert.Equal(float64(math.MaxInt64/512), throughput)

	va.TrackBytes(math.MaxInt64)
	va.TrackBytes(4096)
	iops, throughput = va.Reset(time.Second)
	assert.Equal(2.0, iops)
	assert.Equal(float64(math.MaxInt64/1024+4), throughput)

	// now let's try with some bigger durations

	va.TrackBytes(1024)
	va.TrackBytes(1024)
	va.TrackBytes(1024)
	iops, throughput = va.Reset(time.Second * 30)
	assert.Equal(0.1, iops)
	assert.Equal(0.1, throughput)

	va.TrackBytes(4096)
	va.TrackBytes(4096)
	va.TrackBytes(4096)
	va.TrackBytes(4096)
	va.TrackBytes(4096)
	iops, throughput = va.Reset(time.Second * 5)
	assert.Equal(1.0, iops)
	assert.Equal(4.0, throughput)

	va.TrackBytes(1024)
	va.TrackBytes(2048)
	va.TrackBytes(4096)
	va.TrackBytes(1024)
	iops, throughput = va.Reset(time.Second * 2)
	assert.Equal(2.0, iops)
	assert.Equal(4.0, throughput)
}
