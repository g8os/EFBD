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

The parameters are used to form a zerolog.MsgStatistics struct, used to format a statistics message's message.

More in depth information about zerolog.MsgStatistics can be found in the godocs: https://godoc.org/github.com/zero-os/0-log#MsgStatistics
and the zerolog docs: https://github.com/zero-os/0-log .

AggregationType and MetricTags are wrappers for the zerolog types with the same names
provided to avoid using the 0-log package in other parts of 0-Disk.

An error will be returned in the following cases:

	- The aggregation type is invalid (zerolog.ErrInvalidAggregationType)

usage example without tags:

	BroadcastStatistics("vvdisk.iops.write@virt.a", 268.51483871176936, AggregationAverages, nil)
	// outputs: 10::vdisk.iops.write@virt.a:268.51483871176936|A

usage example with tags:

	tags := MetricTags{"cluster": "a"}
	BroadcastStatistics("vvdisk.iops.write@virt.a", 268.51483871176936, AggregationAverages, tags)
	// outputs: 10::vdisk.iops.write@virt.a:268.51483871176936|A|cluster=a
*/
package log
