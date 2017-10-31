# ARDB failure scenarios

Disks fail, nodes fail and ardb's can crash. What actions does we have to take in case of ardb failure?

It is not as simple as just switching to the slave since redis does asynchronous replication so there is a small window for data loss.

## Failure Scenarios

### a master node fails

#### no slave cluster is supported or configured

When a master node fails and no slave cluster is configured or available,
the vdisk will fail as soon as an operation touches the master node which failed.

#### a slave cluster is supported and configured

When a master node fails and a slave cluster is configured and available,
the following will immediately happen:

1. The TlogServer will be notified to no longer sync to that slave node of that pair until further notice, the configuration of that slave cluster gets reloaded or that slave cluster gets replaced;
2. AYS will be notified via an STDERR log that this master node failed, so that it can start considering what to do next;

As soon as AYS made a decision on what to do, it will update the configuration of the master cluster:

+ If the master node that previously failed was restored:
    1. It will be marked as `repair`;
    2. This will trigger to copy all data for that vdisk, from the slave node to the master node;
    3. The TlogServer will be instructed that it can sync to the slave shard in question once again;
    4. The master (primary) node will be marked as `online` oce again;
    5. AYS will be notified via an STDERR that the primary cluster has been repaired;
+ If the master node was deemed to be offline forever:
    1. It will be marked as `respread`;
    2. This will trigger to respread all data of that node, from the slave server to the primary cluster;
    3. Both the primary and slave node will be marked as `rip` and can be forgotten;
    4. AYS will be notified that the primary and slave node has been marked `rip` for this vdisk;

If in the middle of any of these processes something goes wrong the vdisk will become unusable and the AYS will be notified via an STDERR log to let it know what went wrong. The vdisk will be unmounted in the meanwhile.

#### Summary

+ The server count of the primary and slave cluster will always be equal (anything else is a critical error);
+ When a primary node is marked as `rip` its linked slave node will always be marked as `rip` as wel, and vice versa;
+ Data can be copied between slave and master clusters for restore purposes;
+ Once a server is marked `rip`, it is expected that it will never change state again;

### a slave node fails

When a slave node fails the following will happen:

1. The TlogServer will stop syncing to it and will notify the nbdserver;

As soon as AYS made a decision on what to do, it will update the configuration of the slave cluster:

+ If the slave node that previously failed was restored:
    1. It will be marked as `repair`;
    2. This will trigger to copy all data for that vdisk, from the primary node to the slave node;
    3. The TlogServer will sync to this node once again;
    4. The slave node will be marked as `online` once again;
    5. AYS will be notified via an STDERR that the slave cluster has been repaired;
+ If the slave node was deemed to be offline forever:
    1. The primary server will be marked `rip`;
    2. The data of the server (node) will be respread among the available servers on that primary cluster;
    3. The slave server will be marked `rip` as well;
    4. AYS will be notified via an STDERR log as well that this primary node is now to put to sleep forever (within the context of this storage cluster at least), it will also be notified that the primary and slave node has been marked `rip` for this vdisk as well;

If in the middle of any of these processes something goes wrong the slave cluster will become unusable and the AYS will be notified via an STDERR log to let it know what went wrong.

### a master-slave node pair fails

In this case the data will be replayed using the TLog Player. All data originally stored on the failed master server, will be respread over all the other available servers. Both nodes will be marked as `rip` afterwards.

#### all available tlog servers fail as well

If there are no available tlog servers, whether that's because no tlog server is available or no server is configured is not important, than it is considered a critical error when a primary-slave node pair, and the vdisk will exit with an error as soon as data is tried to be read from it or written to it.

### all master nodes are offline

When all master nodes are offline, the tlogserver will be no longer syncing any data to the slave cluster, and instead the NBDServer will be using the slave cluster as if it was the primary cluster itself.

However as soon as one or multiple master nodes get restored, the primary cluster will be used once again for those nodes that are available.

### all slave nodes are offline

When all slave nodes are offline, any primary node that goes offline and is touched will result in a critical error which exists any vdisk which is touching the offline node.

### all master and slave nodes are offline

This will result in a critical error for any vdisk which is using any of these clusters.

### all nodes are dead

If all nodes are dead, any vdisk that uses this cluster (pair) will no longer be usable until a new cluster is assigned to it.

## Final words

+ There will be a direct mapping between slave and primary nodes, this is to keep things simple;
  + As a consequence this means that if either node goes into the `rip` state, the other will be marked `rip` as well, and both will need their data to be respread across their cluster;
+ In the future a new `zeroctl` command could be added (if desired), which would allow to move data from one cluster to another cluster.  This feature would allow to expand a cluster by respreading the data from the existing servers to the new (expanded) set of servers (existing+new);

Some final words about storage server state flow:

+ An `online` node can only ever marked as `offline`;
+ An `offline` node can be marked either as `respread` or `repair`;
+ A node marked as `repair` can be marked either as `online` or `offline`;
+ A node marked as `respread` can be marked either as `rip` or `offline`;
+ A node marked as `rip` can only ever be marked as `rip`;
+ Any other state flow is assumed to be an error and ignored (after notifying AYS about this config error);
