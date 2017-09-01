/*Package statistics defines a statistics logging API and is to be used for all 0-Disk statistics logging purposes.

Broadcast

Broadcast is a function that wraps 0-log specifically for logging statistical information to the 0-core log monitor.

The parameters are used to form a zerolog.MsgStatistics struct, used to format a statistics message's message.

More in depth information about zerolog.MsgStatistics can be found in the godocs: https://godoc.org/github.com/zero-os/0-log#MsgStatistics
and the zerolog docs: https://github.com/zero-os/0-log .

AggregationType and MetricTags are wrappers for the zerolog types with the same names
provided to avoid using the 0-log package in other parts of 0-Disk.

An error will be returned in the following cases:

	- The vdiskID is empty (ErrNilVdiskID)
	- The Key is invalid (ErrInvalidKey)
	- The aggregation type is invalid (zerolog.ErrInvalidAggregationType)

usage example without tags:

	Broadcast("vdisk1", KeyIOPSWrite, 1.234, AggregationAverages, nil)
	// outputs: 10::vdisk.iops.write@virt.vdisk1:1.234000|A

usage example with tags:

	tags := MetricTags{
		"foo":   "world",
		"hello": "bar",
	}
	Broadcast("vdisk2", KeyTroughputRead, 2.345, AggregationDifferentiates, tags)
	// outputs: 10::vdisk.throughput.read@virt.vdisk2:2.345000|D|foo=world,hello=bar

IOPS and throughput statistics loggers

For logging IOPS and throughput statistics, 2 functions are provided:
StartIOPSThroughputRead for logging the read statistics and StartIOPSThroughputWrite for logging the write statistics.
These functions are used for sending IOPS and throughput statistics based on how many bytes that have been sent to the logger.

When callling these functions, 3 goroutines are created. One that listens for incoming data and collects/aggregates it,
another that at a predetermined interval calculates the IOPS ((part of) blocks per second) and throughput (in kB/s)
based on the data from the aggregating goroutine and broadcasts them using the Broadcast function
and when a valid config source (config.Source) is provided another goroutines is lauched to listen for updates on the cluster ID.
If the source is not valid or nil, an error will be logged and the cluster ID will be omitted
from the tags when the statistics are broadcasted.

The functions return a IOPSThroughputLogger struct that allows for interaction with the logger.
IOPSThroughputLogger.Send provides a way to push data (amount of bytes written or read) to the logger.
IOPSThroughputLogger.Stop cancels the internal context and closes the running goroutines.

usage example:
	blockSize := int64(4096)
	vdiskID := "testVdisk"
	readLogger := StartIOPSThroughputRead(nil, vdiskID, blockSize)
	defer readLogger.Close()

	// log that only half a block was read (2048 bytes)
	readLogger.Send(2048)

	// with an interval set at 1 minute, this will output (when interval is reached):
	// 10::vdisk.iops.read@virt.testVdisk:0.008333333333333333|A
	// 10::vdisk.throughput.read@virt.testVdisk:0.03333333333333333|A
*/
package statistics
