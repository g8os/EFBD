# Go Tlog Server


- TLOG Server store received log entries and store it in memmory
- After storing log entry it replies to the client on successfull transaction.
- after timeout or size of the aggregation is reached, we `Flush` it:
	- aggregate the entries
	- compress the aggregate
	- encrypt the compressed aggregate
	- erasure encode the encrypted aggregate
	- store each pieces of erasure encoded pieces to ardb in parallel way

- Ideal setup would be to spread erasure coded pieces on different ardb instances.
- Each instance is used to keep erasure coded part according to its index (erasure coded part index == ardb instance index)
- We keep only backward links in our blockchain of history. We will add separate forward lining structure later in case it will be needed for the speed of recovery


## Flush Settings

settings directly related to flush:
- flush-size: minimum number of blocks to be flushed (default = 25)
- flush-time: maximum time we can wait entries before flushing it (default = 25 seconds)
- k : number of erasure encoded data pieces
- m : number of erasure encoded coding/parity pieces
- nonce: hex nonce used for encryption 
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

## Metadata

Tlog has very simple metadata, it only need to store hash of the last aggregation.

Because the way we store the data (erasure coded part index == ardb instance index) we don't need
to store which instance we use to store a data.

Tlog server do these to increases reliability:

- store 5 last hashes. In case of the very last hash is corrupted, we can use previous hash
- store the metadata on all ardb instances

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

## erasure coding

It use [isa-l](https://github.com/01org/isa-l) C library and [templexxx/reedsolomon](https://github.com/templexxx/reedsolomon) Go library for erasure coding.

Only one can be used at a time.

By default, the erasure coding is done in go.

When using the C isa-l library for the erasure coding, `-tags isal` needs be passed to go build.
And `GODEBUG=cgocheck=0` environment variable need to be set in order to run it.


**Build**

From this repo root directory
```
make tlogserver
```


**Usage**

Run it
```
./bin/tlogserver  -storage-addresses=127.0.0.1:16379 -k 16 -m 4
```

It starts tlog server that:
- listen on default listen address 0.0.0.0:11211
- need 16 data shards and 4 parity/coding shards -> total 20 ardb 
- first ardb address is `127.0.0.1:16379`, the seconds is in same IP but in port `16380`, the third in port `16381`, and so on...

To specify each ardb adddress, use array as address, example:`-storage-addresses=127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381` to specify 3 ardbs

Use tlog client as described in [client readme](../tlogclient/readme.md) to send transaction log to this tlog server.


**benchmark**
```
GODEBUG=cgocheck=0 go test -tags isal -bench=.
```
