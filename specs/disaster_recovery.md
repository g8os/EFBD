# Disaster recovery

This document explain what happens when both the master and slave cluster fails at the same time.
This event is considered really unlikely and thus make this procedure the last ressort to get the data back online.

## When this procedure should be triggered?
In the case where a shard of the master cluster fails, without a slave shard of the slave cluster available to back it up. Whether that this is because no slave cluster is configured, or because that specific shard of the slave cluster failed as well is of no importance.

In other words, when some data gets unaccessible by any means.

## Proposed solution
When the disaster occurs, the only place left where we can find the data, is the tlog server.

To re-distribute the lost data to the remaining shards of the master cluster, the tlog will have to replay all the vdisks till all the data have been rewritten.
This is needed cause the tlog doesn't have any knowledge of which block belongs to which shards.

This procedure requires all VM to be stopped so no more data is written.
This procedure will be slow, it's a known fact and it's accepted since this is a disaster recovery procedure that should virtally never be needed.

### Implementation
When one of the nbdserver detects a shard down and there is no slave cluster configured. Or if a shard of master cluster and the corresponding shard in slave cluster are innaccesible, nbd server will send a message using 0-log so 0-ochestrator can be aware of the situation.

The 0-orchestrator needs to investigate the problem, based on its finding, the 0-orchestrator needs to update the storage configuration in etcd.
The configuration need to have only working storage server define inside, so while tlog will replay the data, all the working storage server will receive data.

The storage is then consirder unhealthy and all Virtual machine should be stopped.
Once all vm has been stopped, the disaster recovery procedure needs to be trigger manually with a command from `zeroctl` CLI.

Once the recovery procedure is done, all the data from the lost shards should have been redistributed and the storage can be considered as healthy again.

The 0-ochestrator can now restart the virtual machines.

## Details about how tlog need to rewrite the data
Since the tlog has no knowledge of data spreading and doesn't know which data belongs to which shards, we can't ask it to restore only a specific shards. Instead tlog will have to walk over all the known vdisk and so some kind of restore action in order to make sure that all the data for all vdisk are present in the master cluster.

This process involves a lot of data reading and writing, it would be wise we can find a way to only rewrite data that is actually lost and not rewrite the complete data of all shards.

**Possible optimization**:
There is maybe a way to be able to know to which shard a data from tlog belongs.
Idea is to pass 2 configurations to the tlog during the disaster recovery, one configuration is a snapshot of the configuration used before the disaster happened, the other is the configuration after the disaster and the failed shards have been removed.

Using the pre-disaster config, tlog can compute the shards id, and see of it is one of the shard that failed or not. If it's the case, we write the data to a new shard, if not, we skip and continue.