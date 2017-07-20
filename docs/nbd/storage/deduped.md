# Deduped Storage

Deduped storage is the underlying storage type for the [boot vdisk type][boot].

The code for this storage type can be found in [/nbdserver/ardb/deduped.go](/nbdserver/ardb/deduped.go).

You can read the [storage docs](/docs/nbd/storage/storage.md) for more information about other available storage types.

## Storage

Deduped storage stores any given [block][block] only once. Meaning it has no duplicated [blocks][block], hence its name. All [blocks][block] are identified by their [hash][hash], and an [LBA][lba] scheme is used to map each [block index (2)][index] to the correct [block][block] [hash][hash].

![Deduped Storage](/docs/assets/nbd_deduped_storage.png)

For those [vdisks][vdisk] that have [template][template] support, [blocks][block] can also be fetched from the [template server][template] in case they are not available in the primary [storage (1)][storage] cluster.

> WARNING: [template][template] support for a deduped [vdisk][vdisk] _A_ only works if the [metadata (1)][metadata] from the [vdisk](#vdisk) on the used template [storage (1)](#storage) cluster was copied prior to mounting that [vdisk][vdisk] _A_. You can copy a [vdisk][vdisk]'s [metadata (1)][metadata] using the the [zeroctl](#zeroctl) tool's [copy command][cmdcopy].

This storage spreads its data over all available [data (1)][data] [storage (1)][storage] servers. Which specific [storage (1)][storage] server a block is being written to, is defined by following formula:

```
serverIndex = firstByte(hash(block)) % N

where N is the amount of primary/template
data servers specified in the vdisk's config
```

As noted before, deduped blocks are stored on the [data (1)][data] [storage (1)][storage] cluster. On which each [block][block] is stored as an individual value using the [hash][hash] of that [block][block] as its key.

## LBA

The deduped storage makes use of an [LBA][lba] scheme to map all stored [block indices (2)][index] with the [hash][hash] of the deduped content [block][block] it is linked to.

The [LBA][lba] speeds reading up in two different ways:

+ It bundles and stores [hashes][hash] in [sectors][sector]. Each sector contains `128` [hashes][hash], this makes sequential reads/writes much faster.
+ It [caches (2)][cache] sectors in a max capped [LRU cache][lru].

The sector [hash][hash] count of `128` is defined by [`lba.NumberOfRecordsPerLBASector`](/nbdserver/lba/sector.go#L13) constant. A sector has a fixed size of 4 KiB (due to the fact that a [hash][hash] has a fixed size of 32 bytes) and can address 512 KiB (if the block size is 4 KiB). The bigger the block size, the more [data (1)][data] a [sector][sector] can address.

Because the use of [sectors][sector], we need to calculate the `sectorIndex` whenever we want to read/write a hash to/from a `sector`, using the following formula:

```
sectorIndex = blockIndex / N

where N is equal to the
`lba.NumberOfRecordsPerLBASector` constant
```

Meaning that a [hash][hash] for [block index (2)][index] `200` would be stored in the `2nd` sector.

[LBA][lba] sectors are stored together in the [metadata (1)][metadata] [storage (1)][storage] server (of the primary [storage (1)][storage] cluster) in an [ARDB][ardb] hashmap. The key of this hashmap is in the prefixed "`lba:<vdiskID>`" format, while the fields are the sector [indices (3)][index]. The hashmap's key prefix is defined by the [`lba.StorageKeyPrefix`](/nbdserver/lba/lba.go#L323) constant. "`lba:foo`" is an example of such a hashmap key, where the `vdiskID` is "`foo`".

The code for the [LBA][lba] code can be found in [/nbdserver/lba/lba.go](/nbdserver/lba/lba.go).

## Possible Failures

Any read/write operation will fail if:

+ the [metadata (1)][metadata] [storage (1)][storage] server is not reachable, while the required [LBA sector][sector] is not in the [LRU cache][lru] already;
+ a required [LBA sector][sector] is read from the [metadata (1)][metadata] [storage (1)][storage] server is corrupt or ill-formed;
+ a required primary [storage (1)][storage] server is not reachable;
+ a [block][block] cannot be read, because it is not available on the primary [storage (1)][storage] server, and the relevant [template][template] [storage (1)][storage] server which is specified is not reachable;


[ardb]: /docs/glossary.md#ardb
[cache]: /docs/glossary.md#cache
[template]: /docs/glossary.md#template
[metadata]: /docs/glossary.md#metadata
[data]: /docs/glossary.md#data
[vdisk]: /docs/glossary.md#vdisk
[tlog]: /docs/glossary.md#tlog
[storage]: /docs/glossary.md#storage
[block]: /docs/glossary.md#block
[sector]: /docs/glossary.md#sector
[lru]: /docs/glossary.md#lru
[lba]: /docs/glossary.md#lba
[index]: /docs/glossary.md#index
[hash]: /docs/glossary.md#hash
[boot]: /docs/glossary.md#boot
[zeroctl]: /docs/glossary.md#zeroctl
[cmdcopy]: /docs/zeroctl/commands/copy.md
