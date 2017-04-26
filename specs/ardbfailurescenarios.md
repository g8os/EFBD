# ARDB failure scenario's

Disks fail, nodes fail and ardb's can crash. What actions does we have to take in case of ardb failure?

It is not as simple as just switching to the slave since redis does asynchronous replication so there is a small window for data loss.

## NBDServer

### Content
1. Master ardb fails  
**TODO**
2. Slave ardb fails but the master is still alive  
**TODO**
3. Both failed  
**TODO**

### Metadata
1. Master ardb fails  
**TODO**
2. Slave ardb fails but the master is still alive  
**TODO**
3. Both failed  
**TODO**

## TLogServer

**TODO**
