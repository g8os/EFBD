# Non Deduped Storage

Non Deduped storage is the underlying storage type for the [db][db]- and [cache][cache] [vdisk][vdisk] types.

The code for this storage type can be found in [/nbdserver/ardb/nondeduped.go](/nbdserver/ardb/nondeduped.go).

You can read the [storage docs](/docs/nbd/storage/storage.md) for more information about other available storage types.

## Storage

Non Deduped storage stores all [blocks][block] in [ARDB][ardb] hashmaps, one hashmap per [data][data] [storage (1)][storage] server. The key of each hashmap is in the prefixed "`nondedup:<vdiskID>`" format, while the fields are the block [indices (2)][index]. The hashmap's key prefix is defined by the [`ardb.NonDedupedStorageKeyPrefix`](/nbdserver/ardb/nondeduped.go#L220) constant. "`nondedup:foo`" is an example of such a hashmap key, where the `vdiskID` is "`foo`".

![Non Deduped Storage](/docs/assets/nbd_nondeduped_storage.png)

This direct mapping between a [block][block] and its [index (2)][index], means that there is no need for [metadata][metadata] in the Non Deduped storage. Currently there is also no caching involved at any stage, instead the [blocks][block] are directly read/written from/to the [data (1)][data] [storage (1)][storage] servers for _every_ operation.

For those [vdisks][vdisk] that have [template][template] support, [blocks][block] can also be fetched from the [template server][template] in case they are not available in the primary [storage (1)][storage] cluster.

This storage spreads its data over all available [data (1)][data] [storage (1)][storage] servers. Which specific [storage (1)][storage] server a block is being written to, is defined by following formula:

```
serverIndex = blockIndex % N

where N is the amount of primary/template
data servers specified in the vdisk's config
```

## Possible Failures

Any read/write operation will fail if:

+ a required primary [storage (1)][storage] server is not reachable;
+ a [block][block] cannot be read, because it is not available on the primary [storage (1)][storage] server, and the relevant [template][template] [storage (1)][storage] server which is specified is not reachable;

[vdisk]: /docs/glossary.md#vdisk
[db]: /docs/glossary.md#db
[cache]: /docs/glossary.md#cache
[block]: /docs/glossary.md#block
[ardb]: /docs/glossary.md#ardb
[index]: /docs/glossary.md#index
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[storage]: /docs/glossary.md#storage
[template]: /docs/glossary.md#template