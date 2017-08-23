# Go Tlog Server


- TLOG Server store received log entries and store it in memmory
- After storing log entry it replies to the client on successfull transaction.
- after timeout or size of the aggregation is reached, we `Flush` it:
	- aggregate the entries in capnp format
	- store the data to [0-stor](https://github.com/zero-os/0-stor) server using compress, encrypt, 
	  and distribution (erasure encoding) pipelines


## Flush Settings

settings directly related to flush:
- flush-size: minimum number of blocks to be flushed (default = 25)
- flush-time: maximum time we can wait entries before flushing it (default = 25 seconds)
- k : number of erasure encoded data pieces
- m : number of erasure encoded coding/parity pieces
- priv-key: encryption private key

## Tlog Data structure

Tlog data structure in capnp format

Tlog aggregation per vdisk
```
name (Text)          # unused now
size (uint64)        # number of blocks in this aggregation
timestamp (uint64)
vdiskID (uint32)     # vdisk ID
Blocks: List(Block)  
prev: Data           # hash of previous aggregation
```

Tlog block
```
sequence(uint64) 	# sequence number
offset(uint64)
size(uint64)
hash(Data)			# hash of this block's data
data(Data)
timestamp(uint64)
operation			# disk operation
```


## Tlog Server Configuration

See [the Tlog Server Configuration docs](/docs/tlog/config.md) for more information about how to configure the Tlog Server.

## Nbdserver slave sync feature

Tlog server has feature to sync all nbdserver operation to the ardb slave.
In case nbdserver's master failed, nbdserver can switch to this slave.

This feature need this configuration:
- set tlogserver command line `-with-slave-sync` to true. It is false by default
- set `tlogSlaveSync` in vdisk configuration to true. See the example above
- set `storageCluster` to the slave's cluster. See the example above

After nbdserver switch to slave (internally by executing `WaitNbdSlaveSync` command),
the slave sync feature of this vdisk become disabled.
To re-enable this, the vdisk need to be restarted in nbdserver side.

TODO : when hot reload the config, re-enable the slave sync if possible.

## Usage

Use `tlogserver -h` or `tlogserver --help` to get more information about all available flags.

**Build**

From this repo root directory
```
make tlogserver
```


**Usage**

Run it
```
./bin/tlogserver  -k 16 -m 4
```

It starts tlog server that:
- listen on default listen address 0.0.0.0:11211
- need 16 data shards and 4 parity/coding shards -> total 20 0-stor server


Use tlog client as described in [client readme](../tlogclient/readme.md) to send transaction log to this tlog server.

