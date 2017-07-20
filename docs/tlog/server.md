# TLog Server

- The TLog Server store received [log (3)][log] entries and store it in memmory
- After storing log entry it replies to the [client][tlogclient] on successfull transaction.
- After timeout or size of the [aggregation][aggregation] is reached, we store it in persistent memory using the [0-stor-lib][0-stor-lib].

See the [TLog client docs][tlogclient] for more information about how to connect to a running TLog server.

The code for the tlogserver can be found in the [/tlog/tlogserver](/tlog/tlogserver) module.

## Flush Settings

settings directly related to flush:
- `flush-size`: minimum number of blocks to be flushed (default = 25)
- `flush-time`: maximum time we can wait entries before flushing it (default = 25 seconds)
- `k` : number of erasure encoded data pieces
- `m` : number of erasure encoded coding/parity pieces
- `nonce`: hex nonce used for encryption 
- `priv-key`: encryption private key

## TLog Data structure

TLog [data (2)][data] structures are wired and stored in the [Cap'n Proto][capnp] protocol.

TLog [aggregation][aggregation] per [vdisk][vdisk]:

```
name (Text)          # unused now
size (uint64)        # number of blocks in this aggregation
timestamp (uint64)
vdiskID (uint32)     # vdisk ID
Blocks: List(Block)  
prev: Data           # hash of previous aggregation
```

TLog block:

```
sequence(uint64) 	# sequence number
offset(uint64)
size(uint64)
hash(Data)			# hash of this block's data
data(Data)
timestamp(uint64)
operation			# disk operation
```

See the [TLog capnp schema file][tlogschema] for more information and details.

## Metadata

TLog has a very simple [metadata (4)][metadata] structure, and only stores the [hashes][hash] of the last 5 [aggregations][aggregation]. This allows us to use the previous [hash] in case the very last [hash][hash] is corrupted.

## NBD Server slave sync feature

The TLog server has a feature to sync all [NBD][nbd] Server write operations with a [slave][slave] [storage (1)][storage] cluster. This allows the [NBD][nbd] server to use the [slave][slave] [storage (1)][storage] cluster instead of the primary [storage (1)][storage] cluster in case the primary one fails.

This feature requires the following configuration:

- Set the TLog server CLI flag `-with-slave-sync` to `true`, it is `false` by default.
- Set the `tlogSlaveSync` property in the [vdisk][vdisk] [Tlog's configuration][tlogconfig] to `true`.

After the [NBD][nbd] Server switches to the [slave][slave] [storage (1)][storage] cluster (automatically done by executing the internal `WaitNbdSlaveSync` command), the [slave][slave] sync feature of this [vdisk] will become disabled. To re-enable this, the [vdisk][vdisk] needs to be restarted on the [NBD][nbd] Server side.

> TODO: when the config is reloaded on the fly (see: [hotreload][hotreload]), re-enable the [slave][slave] sync if possible.

## Usage

```
$ tlogserver -h
tlogserver 1.1.0-alpha

Usage of bin/tlogserver:
  -address string
    	Address to listen on (default "0.0.0.0:11211")
  -block-size int
    	block size (bytes) (default 4096)
  -config string
    	Zerodisk Config YAML File (default "config.yml")
  -flush-size int
    	flush size (default 25)
  -flush-time int
    	flush time (seconds) (default 25)
  -k int
    	K variable of the erasure encoding (default 4)
  -logfile string
    	optionally log to the specified file, instead of the stderr
  -m int
    	M variable of the erasure encoding (default 2)
  -memorystorage
    	Stores the (meta)data in memory only, usefull for testing or benchmarking (overwrites the storage-addresses flag)
  -nonce string
    	hex nonce used for encryption (default "37b8e8a308c354048d245f6d")
  -priv-key string
    	private key (default "12345678901234567890123456789012")
  -profile-address string
    	Enables profiling of this server as an http service
  -storage-addresses string
    	comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config
  -v	log verbose (debug) statements
  -with-slave-sync
    	sync to ardb slave
```


[tlogclient]: client.md
[tlogplayer]: player.md
[tlogconfig]: config.md
[tlogschema]: /tlog/schema/tlog_schema.capnp

[log]: /docs/glossary.md#log
[aggregation]: /docs/glossary.md#aggregation
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[hash]: /docs/glossary.md#hash
[nbd]: /docs/glossary.md#nbd
[storage]: /docs/glossary.md#storage
[vdisk]: /docs/glossary.md#vdisk
[slave]: /docs/glossary.md#slave
[hotreload]: /docs/glossary.md#hotreload

[0-stor-lib]: https://github.com/zero-os/0-stor-lib

[capnp]: http://capnproto.org
