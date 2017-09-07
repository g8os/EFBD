# Stderr Logging

Using the [0-Log library][zerolog] we can log to the `stderr`/`stdout` messages for statistics, failures and any other information services subscribes to the [0-core log monitor][zeroCoreLogMonitor] would want to receive.

## Broadcasting to 0-Orchestrator

The `Broadcast` function in the `0-Disk/log` package logs messages using the [0-Log library][zeroLog] to broadcast any message the [0-Orchestrator][zeroOrchestrator] should be aware of. While in most cases the 0-disk services won't stop working because of the occured failures, it is none the less very important that [0-Orchestrator][zeroOrchestrator] handles these messages as quickly as possible, in case intervention is required.

For more in-depth information about the actual implementation in 0-Disk, you can read [the log module Godocs][zeroDiskLogGodcs].

### Message Format

All broadcasted messages are send using [level 20 (loglevel JSON)][loglevels] and use the following json format:

```js
{
    "subject": string, // Subject
    "status": integer, // Status Code
    "data": object,    // Body (data)
}
```

The (message) subject defines where the message orginates from,
while the (message) status code defines what is happening in that origin.

```js
{
    "subject": "etcd",       // Subject
    "status": 401,           // Status Code
    "data": ["1.1.1.1:22"],  // Body (data)
}
```

The message above for example originates from `etcd`,
or at least our usage of it, indicated by the (message) `subject`.
While the status code of the example above indicates we have a `cluster timeout`.
The message type is identified by the combination of its status code and subject.
What the data's format is and what it contains will be clear
once you identified what the message is about.

It is important to never try to identify a message
by using only its `status code` or `subject`.
A `status code` is used by multiple `subjects`,
and a (message) `subject` is used by multiple `status codes`.
Therefore make sure to always check both the `status code` and the `subject`.
As the `status code` is only a 32bit (unsigned) integer and the
`subject` is a string of maximum 8 characters, this isn't an expensive thing to do.

Thanks to the [0-Log library][zerolog] the final output of the example above would be:

```
20::{"subject":"etcd","status":401,"data":["1.1.1.1:22"]}
```

#### Status Codes

| status code  | meaning |
| ----- | ------- |
| `400` | generic/unknown error |
| `401` | cluster time out |
| `403` | invalid config |
| `421` | server timeout |
| `422` | server disconnect |
| `423` | server temporary error |

#### Status Subjects

| subject  | related to: |
| ----- | ------- |
| `ardb` | (our usage of) an [ardb][ardb] server/cluster |
| `etcd` | (our usage of) an [etcd][etcd] server/cluster |
| `zerostor` | (our usage of) an [zerostor][zerostor] server/cluster |

### Messages

What follows is a list of all possible messages broadcasted by 0-Disk services,
including their format, its reason and what we expect to be done about it (by the [0-Orchestrator][zeroOrchestrator]).

#### ardb storage server issues

```js
{
    "subject": "ardb",       // ardb
    "status": 421,           // all possible status codes: { 400, 421, 422, 423 }
    "data": {
        "address": "1.2.3.4:16379", // address of ardb server
        "db": 41,                   // database index of ardb server
        "type": "template",         // ardb server type, options: {primary, slave, template}
        "vdiskID": "vd2",           // vdiskID this server ardb server was used for
    },
}
```

Sent when we get a permanent/temporary (net) timeout, while trying to apply one or multiple operations on an ARDB server.
It is for example sent when we can't fetch content from a template server due to a connection timeout.

This is a critical failure and can't be restored from in most cases,  without intervention from the [0-Orchestrator][zeroOrchestrator].

This message is send in the hope that the ardb server can come back online, ready for use by the 0-Disk services in question,
or if that is not possible any other solution that makes it possible again to recover (from) the lost functionality.

#### etcd cluster time out

```js
{
    "subject": "etcd",       // etcd
    "status": 401,           // cluster time out
    "data": ["1.1.1.1:22"],  // endpoints of the cluster
}
```

Sent when we get a time out while trying to setup or use a connection to/of an etcd cluster. It is for example sent when fail to read a config stored in an etcd cluster, because we get a time out while doing so.

This is a critical failure and can't be restored from in most cases, without intervention from the [0-Orchestrator][zeroOrchestrator]. One of the consequences of the [etcd][etcd] cluster being down, is that no new [vdisk][vdisk] can be mounted until the cluster is back online. 

This message is send in the hope that the etcd cluster can come back online, ready for use by the 0-Disk services in question.

#### zerostor cluster time out

```js
{
    "subject": "zerostor",   // 0-stor
    "status": 401,           // cluster time out
    "data": ["1.1.1.1:22"],  // endpoints of the cluster
}
```

Sent when we get a time out while trying to setup or use a connection to/of a 0-stor cluster.
It is for example sent when get a timeout error while trying to flush content to the 0-stor cluster in question.

This is a critical failure and can't be restored from without intervention from the [0-Orchestrator][zeroOrchestrator].

This message is send in the hope that the 0-stor cluster can come back online, ready for use by the 0-Disk services in question.
Or if that is not possible, that it can be replaced by another 0-stor cluster which is active and ready for useage.

#### zerostor cluster unknown error

```js
{
    "subject": "zerostor",   // 0-stor
    "status": 400,           // unknown error
    "data": ["1.1.1.1:22"],  // endpoints of the cluster
}
```

Sent when we get an unknown error while trying to setup or use a connection to/of a 0-stor cluster.
It is for example sent when get an unknown error while trying to flush content to the 0-stor cluster in question.

This is a critical failure and can't be restored from without intervention from the [0-Orchestrator][zeroOrchestrator].

This message is send in the hope that the 0-stor cluster can come back online, ready for use by the 0-Disk services in question.
Or if that is not possible, that it can be replaced by another 0-stor cluster which is active and ready for useage.

#### received an invalid config from an etcd cluster

```js
{
    "subject": "etcd",  // etcd
    "status": 403,      // invalid config
    "data": {
        // endpoints of the etcd cluster
        "endpoints": ["1.1.1.1:22"], 
        // (etcd) config key
        "key": "mycluster:cluster:conf:storage", 
        // optional: the ID of the vdisk is only given
        // in case the config is only invalid because
        // the vdisk in question has certain
        // unfulfilled expectations of this
        // sub-configuration.
        "vdiskID": "vd2",
    },
}
```

Sent when receiving an invalid config for a certain key, while reading or watching that key. If this happens during an update while watching this key, we'll stick with the config as it was, such that nothing breaks down as the old configuration is still valid for usage. If the old config is no longer usable or we are reading the given key (initially), this will however result in a critical failure and it might potentially shut down the [vdisk][vdisk]'s session it is used for.

This message is send in the hope that the config can be made valid by receiving an(other) update from the [0-Orchestrator][zeroOrchestrator].

## Broadcast statistics 

The `BroadcastStatistics` function in the `0-Disk/log` package logs statistical messages using the [0-Log library][zeroLog] to broadcast messages for the [0-core log monitor][zeroCoreLogMonitor] using the [Statistics Log message format spec][StatLogSpec]. The broadcasted statistics messages are send at [log level 10 (statistics/monitoring message)][loglevels]. 

More in-depth information about the actual implementation in 0-Disk can be found in [the log module Godocs][zeroDiskLogGodcs].

### Logged statistics

 * [vdisk][vdisk] IOPS read 
    * logged by: [nbdserver][nbdserver]
    * broadcasts: `10::vdisk.iops.read@virt.<vdiskID>:<value>|A|cluster=<clusterID>,templateCluster=<templateClusterID>`
    * [0-core aggregation type][StatLogSpec]: Averages
    * value unit: (partial) blocks per second
    * metric tags:
        * `cluster`: the primary storage cluster ID (required)
        * `templateCluster`: the template storage cluster ID (optional, not given when not defined)
    * logging interval: 30 seconds (or less in case the vdisk unmounts before an interval ends)
 * [vdisk][vdisk] IOPS write
    * logged by: [nbdserver][nbdserver]
    * broadcasts: `10::vdisk.iops.write@virt.<vdiskID>:<value>|A|cluster=<clusterID>,templateCluster=<templateClusterID>`
    * [0-core aggregation type][StatLogSpec]: Averages
    * value unit: (partial) blocks per second
    * metric tags:
        * `cluster`: the primary storage cluster ID (required)
        * `templateCluster`: the template storage cluster ID (optional, not given when not defined)
    * logging interval: 30 seconds (or less in case the vdisk unmounts before an interval ends)
 * [vdisk][vdisk] Throughput read
    * logged by: [nbdserver][nbdserver]
    * broadcasts: `10::vdisk.throughput.read@virt.<vdiskID>:<value>|A|cluster=<clusterID>,templateCluster=<templateClusterID>`
    * [0-core aggregation type][StatLogSpec]: Averages
    * value unit: KiB/s
    * metric tags:
        * `cluster`: the primary storage cluster ID (required)
        * `templateCluster`: the template storage cluster ID (optional, not given when not defined)
    * logging interval: 30 seconds (or less in case the vdisk unmounts before an interval ends)
 * [vdisk][vdisk] Throughput write
    * logged by: [nbdserver][nbdserver]
    * broadcasts: `10::vdisk.throughput.write@virt.<vdiskID>:<value>|A|cluster=<clusterID>,templateCluster=<templateClusterID>`
    * [0-core aggregation type][StatLogSpec]: Averages
    * value unit: KiB/s
    * metric tags:
        * `cluster`: the primary storage cluster ID (required)
        * `templateCluster`: the template storage cluster ID (optional, not given when not defined)
    * logging interval: 30 seconds (or less in case the vdisk unmounts before an interval ends)

More details over the nbd server statistics logging can be found in the [nbd server statistics module godocs][zeroDiskStatisticsGodcs]

[zeroLog]: https://github.com/zero-os/0-log/
[loglevels]: https://github.com/zero-os/0-log/blob/master/README.md#supported-log-levels
[zeroCoreLogMonitor]: https://github.com/zero-os/0-core/blob/master/docs/monitoring/README.md#monitoring
[zeroOrchestrator]: https://github.com/zero-os/0-orchestrator
[StatLogSpec]: https://github.com/zero-os/0-core/blob/master/docs/monitoring/stats.md#statistics-log-message-format

[ardb]: /docs/glossary.md#ardb
[tlog]: /docs/glossary.md#tlog
[etcd]: /docs/glossary.md#etcd
[vdisk]: /docs/glossary.md#vdisk

[nbdserver]: /docs/nbd/nbd.md

[zerostor]: https://github.com/zero-os/0-stor

[zeroDiskLogGodcs]: https://godoc.org/github.com/zero-os/0-Disk/log
[zeroDiskStatisticsGodcs]: https://godoc.org/github.com/zero-os/0-Disk/nbd/nbdserver/statistics