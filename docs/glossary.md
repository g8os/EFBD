# Glossary

## [ A - J ]

### AGGMQ

The [Aggregation](#aggregation) Message Queue (AGGMQ) is used to queue [aggregations](#aggregation) in the [TLog](#tlog) server as they are waiting to be processed. See the [TLog docs][tlog] for more info.

### aggregation

An aggregation consists out of a group of [TLog](#tlog) sequences and some metadata (such as the [vdisk](#vdisk)'s identifier). See the [TLog docs][tlog] for more info. The aggregation definition can be found as a [Cap'n Proto][capnp] schema [tlog_schema.capnp][tlogschema].

### ARDB

ARDB (or "A Redis Database") is the collective term used for the underlying storage where the [persistent](#persistent) [vdisks](#vdisk)' [data (1)](#data) and [metadata (1,2,3)](#metadata) are stored. In production [redis][redis] is used, while [LedisDB][ledis] is used for test- and dev purposes. See the [NBD storage docs][storage] for more info. Usually a group of servers (called a cluster) are used together, in order to spread the data and processing.

### backend

[NBD](#nbd) backend is the core implementation of all [volume drivers](#volume-driver) used via the [NBD server](#nbd). It is used as glue between the [gonbdserver](/nbd/gonbdserver) and the actual underlying [NBD storage (2)](#storage). See the [NBD docs][nbd] for more info.

### backup

Using the [zeroctl](#zeroctl) tool, one can make a backup of a [vdisk](#vdisk) using the [export command][cmdexport]. The backup can then be restored again using the [import command][cmdimport], either as a new [vdisk](#vdisk), or overwriting an existing [vdisk](#vdisk).

### block

All [vdisks](#vdisk)' [data (1)](#data) is stored as blocks, with a dynamic size usually measured in bytes. 4096 KiB is the standard size for [boot](#boot) [vdisks](#vdisk). See the [NBD storage docs][storage] for more info.

### boot

Boot is one of the available [vdisk](#vdisk) types. It uses the [deduped storage](#deduped) as its underlying [storage (2)](#storage) type. [Data (1)](#data) and [metadata (1,3)](#metadata) are redundant by making use of the [TLog](#tlog) client. The boot type is meant for (OS) images, hence the name. See the [NBD docs][nbd] for more info.

### cache

1. Cache is one of the available [vdisk](#vdisk) types. It uses the [nondeduped storage](#nondeduped) as its underlying [storage (2)](#storage) type. As its name suggests it is meant for caching. See the [NBD docs][nbd] for a more elaborabe overview.

2. A Least Recently Used (LRU) cache is used to cache loaded [LBA sectors](#sector). The cache only keeps a low amount of [sectors](#sector) in memory, and evicts the [sector](#sector) last used in case the cache is full.

### data

1. Data in the context of [vdisks](#vdisk) refers usually to the actual [blocks](#block), which are [stored](#storage) on an [ARDB](#ardb) cluster. See the [NBD docs][nbd] for more info.

2. [TLog Transactions](#tlog) are sometimes also refered to as data, especially in the context of the [TLog server][tlogserver].

### db

Database (db) is one of the available [vdisk](#vdisk) types. It uses the [nondeduped storage](#nondeduped) as its underlying [storage (2)](#storage) type. [Data (1)](#data) and [metadata (3)](#metadata) are redundant by making use of the [TLog](#tlog) client. The boot type is meant for (OS) images, hence the name. See the [NBD docs][nbd] for more info.

### deduped

[Blocks](#block) stored in the deduped [storage (2)](#storage) are only stored once, and are identified by their block [hash](#hash), which is referenced via its [metadata (1)](#metadata). See the [deduped storage docs][dedup] for more info.

### etcd

[etcd][etcd] is a distributed reliable key-value store and uses the Raft consensus algorithm to manage a highly-available replicated log.

Configurations for all [vdisks](#vdisk) to be mounted by a given [NBD Server][nbd] and supported by other 0-Disk services such as the [TLog Server][tlogserver], are stored in an [etcd][etcd] cluster. [0-Orchestrator][0-Orchestrator] sets up this cluster and writes all these configurations to the cluster. 0-Disk services consider this cluster as read-only. For more information about this use case of [etcd][etcd] check out the [etcd config docs][etcdConfigDocs].

### hash

A hash function is any function that can be used to map data of arbitrary size to data of fixed size. In the case of 0-Disk it can be assumed that the [blake2b][blake2b] cryptographic hashing algorithm is used, unless explicitly specified otherwise. A hash is the name for the fixed-sized data produced by the hashing algorithm.

### hotreload

Any 0-Disk config supports hot reloading as an optional feature, for where it is needed. Meaning that the config can be reloaded without having to restart the application/process using it. See the [config docs][config] for more info.

### index

1. Sequence index is used to identify a [TLog](#tlog) sequence in order. The index is automatically asigned by the [TLog Server][tlogserver].

2. [Block](#block) index is used to identify a [vdisk](#vdisk)'s [block](#block). The index can be calculated using the formula `i = position % blockSize`, where both the position and blockSize are expressed in bytes. 

3. [LBA](#lba) [sector](#sector) index is used to identify an [LBA](#lba) index. The index can be calculated using the formula `i = blockIndex % 128`.

## [ K - T ]

### LBA

Logic Block Addressing (LBA) is the scheme used for specifying the location of [deduped](#deduped) [blocks](#block), by mapping each existing [block](#block) [index (2)](#index) to the [hash](#hash) of that [block](#block). This scheme is [stored (1)](#storage) as [metadata (3)](#metadata). See the [deduped storage docs][dedup] for more info.

### log

1. Debug, Info and Error logs are supported and is done using the [0-Disk/log](/log) module. By default these are logged to the _STDERR_, but both the [NBD server](#nbd) and the [TLog server](#tlog) support logging to a file, if given a path to write to.

2. 0-Disk components are just a small piece of the Zero-OS ecosystem. In case a 0-Disk component (such as [NBD server](#nbd) or [TLog server](#tlog)) has to communicate to other components (such as [0-Orchestrator][0-Orchestrator]), it does so using the [zerolog][0-log] package, which provides formatted _STDERR_ logging, for exactly this purpose of providing a communication medium between Zero-OS components. Read more information about this in [the log docs][logDocs].

3. [Transactions are logged](#tlog) are logged for [vdisks](#vdisk) which support it and is enabled. This is an option for both [db](#db) and [boot](#boot) [vdisks](#vdisk).

### metadata

1. A [Deduped](#deduped) [vdisk](#vdisk) [stores (2)](#storage) the [LBA](#lba) scheme as metadata.

2. [SemiDeduped](#semideduped) [vdisk](#vdisk) [stores (2)](#storage) a bitmap, which indicates what underlying [storage (2)](#storage) is used to store a [block](#block), as metadata.

3. If a supported [vdisk](#vdisk) is [TLog](#tlog)-enabled, it stores some (extra) metadata. See the [TLog storage docs][tlogstorage] for more info.

4. The [TLog server](#tlog) stores its (critical) metadata per [vdisk](#vdisk) in the [0-stor storage (3)](#storage). See the [TLog docs][tlog] for more info.

### NBD

Network Block Device is the name for the Linux-originated protocol as described in [this specification document][nbdproto]. It allows us to mount [vdisks](#vdisk) as if they were a physical block device. When a client connects to the nbdserver it is being handled by the [gonbdserver](/gonbdserver), which on its turn delegates the actual storage work onto the [nbd backend](#backend).

### nondeduped

[Blocks](#block) stored in the nondeduped [storage (2)](#storage) are stored, identified directly using their [block index (2)](#index). Because of this no [metadata](#metadata) is required for the storage of nondeduped [blocks](#block). See the [nondeduped storage docs][nondedup] for more info.

### persistent

[VDisk](#vdisk) [data (1)](#data) and [metadata (1,2,3)](#metadata) are made persistent by storing it in one or multiple [ardb clusters](#ardb). This is the case for all [vdisk](#vdisk) types.

### player

The [TLog](#tlog) player can read the [logged (3) transactions](#log) and use it to [restore](#restore) a [vdisk](#vdisk) by writing it directly to the [NBD backend](#backend). This player is used by the [zeroctl](#zeroctl) restore command for example. See the [TLog Player docs][tlogplayer] for more info.

### pool

In order to limit the amount of connections to a single [storage (1)](#storage) server, a pool is used. Meaning that for example only 10 connections at the same time can be established. Note that that this pool is only limiting at a local level. Each mounted [vdisk](#vdisk) for example has its own pool _per_ [storage (1)](#storage) server, therefore if that vdisk uses 5 [storage (1)](#storage) servers, it will also use 5 pools. Another mounted [vdisk](#vdisk) will never share pools of another [vdisk](#vdisk), even if they use the same servers. The [NBD](#nbd) and [TLog](#tlog) modules have each their own pool implementation.

### redundant

Both the [boot](#boot)- and [db](#db) [vdisk](#vdisk) types have support to make the [data (1)](#data) and [metadata (1,2,3)](#metadata) redundant:

+ [TLog](#tlog) rpc addresses can be given in the [vdisk](#vdisk)'s [TLog config][tlogconfig];
+ When [TLog](tlog) support is enabled, a [slave](#slave) [storage (1)](#storage) cluster also be specified in the [vdisk](#vdisk)'s [TLog config][tlogconfig];
+ A [vdisk](#vdisk) can also make a [backup](#backup) of itself;

### replay

All [logged (3)](#log) ([TLog](#tlog)) transactions can be played back in sequence using the [TLog Player](#player) up to any given time or sequence in tracked history, usually done so using the [zeroctl](#zeroctl) tool.

### restore

[Redundant](#redundant) content can be restored using the [TLog Player](#player) to any given time or sequence in tracked history, usually done so using the [zeroctl](#zeroctl) tool.

### rollback

[Redundant](#redundant) content can be rolled back to any given time or sequence in tracked history, usually done so using the [zeroctl](#zeroctl) tool. See the [tlog docs][tlog] for more info.

### semideduped

Semi Deduped storage is a hybrid storage, using read-only [deduped storage](#deduped) to store the [template](#template) content, and [nondeduped storage](#nondeduped) to store any content (modifications) written by the user. See the [semideduped storage docs][semidedup] for more info.

### sector

An [LBA](#lba) sector contains 128 hashes, and is kept in an LRU [cache (2)](#cache) when it is loaded because one of those hashes were required, so the sector is available for more reads if needed. See the [deduped docs][dedup] for more info.

### slave

A slave [storage (1)](#storage) cluster is a mirror of a vdisk's primary [storage (1)](#storage) cluster by the [TLog server][tlogserver]. Such that in case of failure the primary [storage (1)](#storage) cluster can be restored using the slave [storage (1)](#storage) cluster.

### storage

1. An [ARDB cluster](#ardb) is used as the [persistent](#persistent) storage for all [vdisks](#vdisk) mounted using the [NBD server](#nbd). Only the primary storage cluster (usually shortened to 'storage cluster') is required. The [TLog](#tlog) cluster is required in case you want to make use of it for those [vdisks](#vdisk) that support it. In such case you can also optionally make use of the [Slave](#slave) cluster. Optionally you can also make use of a [Template](#template) cluster for those [vdisks](vdisk) that support it.

2. [NBD backend](#backend) storage types define how a [persistent](#persistent) [vdisk](#vdisk)'s [data (1)](#data) and [metadata (1,2,3)](#metadata) is stored in an [ARDB cluster](#ardb).

3. [0-stor][0-stor] is used to store [TLog](#tlog) [data (2)](#data) and [metadata (4)](#metadata), using the [0-stor-lib][0-stor-lib].

### template

A [boot](#boot) [vdisk](#vdisk) can make use of a template [storage (1)](#storage) cluster. From this cluster [blocks](#block) can be copied on-the-fly into the primary [storage (1)](#storage) cluster in case the latter doesn't have the required [block](#block) available. Note that this only works for a [boot](#boot) vdisk if it was created as a ([metadata (1)](#metadata)) copy from the [vdisk](#vdisk) on that template [storage (1)](#storage) cluster using the the [zeroctl](#zeroctl) tool's [copy command][cmdcopy].

A [db](#db) [vdisk](#vdisk) can also make use of a template [storage (1)](#storage) cluster. However, it does not have any [metadata](#metadata) and thus does not required being copied first. Instead you simply declare a non-existent [db](#db) [vdisk](#vdisk) (with template [storage (1)](#storage) cluster defined) in the [config][config], and the [blocks](#block) will be copied into the primary [storage](#storage) cluster on-the-fly, the first time they are being read.

### TLog

The Transaction [Log (3)](#log) (TLog) module provides a [server][tlogserver], [client][tlogclient] and player. Its purpose is to make [vdisk](#vdisk)'s [data (1)](#data) and [metadata (1,2,3)](#metadata) [redundant](#redundant) by storing all write transactions applied by the user in a seperate [storage (3)](#storage) in a secure and efficient manner.

Both the [boot](#boot)- and [db](#db) [vdisk](#vdisk) types have TLog support. [VDisks](#vdisk) of this type can enable it by defining a TLog [storage (1)](#storage) cluster in its configuration.

### tmp

tmp (short for Temporary) is one of the available [vdisk](#vdisk) types. It uses the [nondeduped storage](#nondeduped) as its underlying [storage (2)](#storage) type. Its [data (1)](#data) is persistent as long as it mounted via the [NBD server](#nbd). The [vdisk](#vdisk) is deleted as soon as it is unmounted. See the [NBD docs][nbd] for a more info..

## [ U - Z ]

### vdisk

A Virtual Disk (vdisk) is the 0-Disk component which emulates an actual [block](#block) [storage (2)](#storage) device. The actual [data (1)](#data) and [metadata (1,2,3)](#metadata) of the vdisk is stored in an [ardb cluster](#ardb). Using the [NBD server](#nbd) it can be mounted and used as if it was a physical disk.

### volume driver

A volume driver defines the actual properties of a [vdisk](#vdisk). A driver is identified by a type. Examples of [vdisk](#vdisk) types are: [boot](#boot), [cache (1)](#cache) and [db](#db).

### zeroctl

The 0-Disk command line tool, used to manage [vdisks](#vdisk). See the [zeroctl docs][zeroctl] for more info.


[redis]: http://redis.io
[ledis]: http://ledisdb.com
[capnp]: https://capnproto.org
[blake2b]: github.com/minio/blake2b-simd
[nbdproto]: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

[0-Orchestrator]: https://github.com/zero-os/0-Orchestrator
[0-stor-lib]: https://github.com/zero-os/0-stor-lib
[0-stor]: https://github.com/zero-os/0-stor
[0-Log]: https://github.com/zero-os/0-log

[config]: /docs/config.md
[etcdConfigDocs]: /docs/config.md#etcd

[logDocs]: /docs/log.md

[nbd]: /docs/nbd/nbd.md
[storage]: /docs/nbd/storage/storage.md
[nondedup]: /docs/nbd/storage/nondeduped.md
[semidedup]: /docs/nbd/storage/semideduped.md
[dedup]: /docs/nbd/storage/deduped.md
[tlogstorage]: /docs/nbd/storage/tlog.md

[tlog]: /docs/tlog/tlog.md
[tlogschema]: /tlog/schema/tlog_schema.capnp
[tlogserver]: /docs/tlog/server.md
[tlogclient]: /docs/tlog/client.md
[tlogplayer]: /docs/tlog/player.md
[tlogconfig]: /docs/tlog/config.md

[zeroctl]: /docs/zeroctl/zeroctl.md
[cmdcopy]: /docs/zeroctl/commands/copy.md
[cmdexport]: /docs/zeroctl/commands/export.md
[cmdimport]: /docs/zeroctl/commands/import.md

[etcd]: https://github.com/coreos/etcd
