/*Package log defines a complete logging API and is to be used for all 0-Disk info/error logging purposes.

Broadcast

Broadcast is a function that wraps 0-log specifically for logging errors in JSON format to the 0-Orchestrator.

It takes a MessageStatus, MessageSubject and an interface for additional data that needs to be sent.
These values are used to form the Message struct which will be send over to zerolog.Log to be logged.

MessageStatus is a status code that represents the status of the MessageSubject.

MessageSubject represents who the status applies to (storage(ardb), etcd, tlog).

No error is returned as this would be used as a last resort to notify the 0-Orchestrator to intervene.

More information about Broadcast can be found in the 0-Disk docs: https://github.com/zero-os/0-Disk/blob/master/docs/log.md#broadcasting-to-0-orchestrator
and more information about 0-log can be found at the 0-log github page: https://github.com/zero-os/0-log

BroadcastStatistics

BroadcastStatistics is a function that wraps 0-log specifically for logging statistical information to the 0-core log monitor.

It takes a vdiskID string, StatisticsKey, value of float64, op AggregationType and a MetricTags.

These values are used to form a zerolog.MsgStatistics struct, used to format a statistics message's message.
The vdiskID and StatisticsKey are used to form the zerolog.MsgStatistics.Key .
value is used for zerolog.MsgStatistics.Value
op is used to set the aggregation operation for the statistic at zerolog.MsgStatistics.Operation .
MetricTag is optional and is used for zerolog.MsgStatistics.Tags

More in debth information about zerolog.MsgStatistics can be found in the godocs: https://godoc.org/github.com/zero-os/0-log#MsgStatistics
and the zerolog docs: https://github.com/zero-os/0-log .

StatisticsKey and AggregationType are wrappers for the zerolog types with the same names
provided to avoid using the 0-log package in other parts of 0-Disk.

An error will be returned in the following cases:

	- The vdiskID is empty (ErrNilVdiskID)
	- The StatisticsKey is invalid (ErrInvalidStatisticsKey)
	- The aggregation type is invalid (zerolog.ErrInvalidAggregationType)

usage example without tags:

	BroadcastStatistics("vdisk1", StatisticsKeyIOPSWrite, 1.234, AggregationAverages, nil)
	// outputs: 10::vdisk.iops.write@virt.vdisk1:1.234000|A

usage example with tags:

	tags := MetricTags{
		"foo":   "world",
		"hello": "bar",
	}
	BroadcastStatistics("vdisk2", StatisticsKeyTroughputRead, 2.345, AggregationDifferentiates, tags)
	// outputs: 10::vdisk.throughput.read@virt.vdisk2:2.345000|D|foo=world,hello=bar
*/
package log
