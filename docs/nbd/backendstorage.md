# Backend Storage

All backend and storage code can be found in the [ARDB][ardb] sub module.

[The Backend][ardb.backend] is used for _all_ nbdserver's incoming NBD requests. The underlying storage however can be different.

## Deduped Storage

[The Duduped Storage][ardb.deduped] stores an [LBA][lba] hashmap per [vdisk][vdisk]. The blocks of the content are stored directly into storage, identified by their representative [Blake2B Hash][blake2b.hash]. Deduped [vdisks][vdisk] don't access this content directly. Instead the content is accessed via the [vdisk][vdisk]'s [LBA][lba]. When an operation is wanted on a block, this is done via its index (the block index). In the [LBA][lba]'s hashmap, we can find maximum one hash per index. In case an index has no content, it is linked to a [NilHash][lba.nilhash] instead (which is zero-filled).

Let's visualise the simplest operation that can be applied onto a block, getting the content:

![(*ardb.dedupedStorage).Get](/docs/assets/deduped_get.png)

In the flow graph above we can see the deduped [vdisk][vdisk]'s main logic theme in action. When retrieving a block we'll always first need to retrieve the hash (identifying the content) from the metadata server, before fetching the actual content (data blocks) from the (local or remote) data server.

When setting (`Set`), modifying (`Merge`) or deleting (`Delete`) the content, we'll access the [LBA][lba] first, in order to get the current Hash. With this hash in hand, we'll delete the content itself. And only then we set, replace or delete the hash itself. This order helps to guarantee that the deduped [vdisk][vdisk]'s [LBA][lba] points to existing content.

Note that the (read-only) root storage is only accessed when content is fetched (which is not available in the local storage). The NBDServer never sets, merges or deletes content stored in the root storage, hence read-only. Note that there at present no mechanism in place to ensure this read-only option for storage clusters, from the persective of the nbdserver.

This storage is called deduped, because no duplicated content is stored. [Hash][blake2b.hash] collisions would overwrite existing content, and could lead to data corruption. Undesired as this is however, it is not likely to happen, due to the fact that not all content is stored in the same ardb, and instead spread over an entire cluster.

It is possible (and in fact desired), that multiple block indices of the (same or different) [vdisk][vdisk] point to the same [hash][blake2b.hash] and thus content.

See [/docs/nbd/lbalookups.md](/docs/nbd/lbalookups.md) more information about the LBA lookups (metadata).

See [the deduped code](/nbdserver/ardb/deduped.go) for more information.

## Nondeduped Storage

Not all [vdisks][vdisk] managed by the NBDServer are deduped. Databases and caches are stored as nondeduped [vdisks][vdisk] for example.

Nondeduped [vdisks][vdisk] store, modify, access and delete their content directly in/from the local storage. All content of a single [vdisk][vdisk] is stored within the same hash map. Each content (block) is identified by the block index itself (field), within the namespace of the [vdisk][vdisk] (vdiskID, the key of the hashmap):

```
key = <vdiskid>      # Eg.: `myvdisk`
field = <blockIndex> # Eg.: `42`
key[field] = <block> # a block is a raw byte slice a fixed
                     # length (the predefined block length)
```

This makes the [nondeduped storage][ardb.nondeduped] and its operations very straightforward. 

See [the nondeduped code](/nbdserver/ardb/nondeduped.go) for more information.

## Tlog Storage

All vdisk types which support the `replay/restore` feature, have their internal storage type wrapped in the [tlog storage][ardb.tlog] type. That is to say, the backend will for those types communicate with the [tlog storage][ardb.tlog] rather then the actual underlying storage (for example [deduped storage][ardb.deduped]).

All content stored in the underlying storage has strong consistency, and it is guaranteed that all content which is stored on the underlying storage, is safely stored as transactions on the tlog server. Meaning that from that moment on, the content can be restored to any stored point in time.

To get a better idea on how the tlog storage works, here is its flow graph:

![tlogStorage](/docs/assets/tlog_storage.png)

As you can see, when modifying content it is stored in the sequence cache, and sent as a transaction to the tlog server. This way, each step is recorded in the tlog server, while at the same time the modified content is already available in the cache for immediate usage.

Content is stored in the sequence cache, linked to its sequence index and block index. When retrieving content from the sequence cache it always returns the content with the highest sequence index, available for that block index. Content is only removed from the sequence cache, when the tlog server confirms that the relevant transactions have been securely stored.

See [the tlog storage code](/nbdserver/ardb/tlog.go) for more information about its internal workings.

See [the tlog docs](/docs/tlog/tlog.md) for more information about the tlog ecosystem.

[ardb]: /nbdserver/ardb
[ardb.backend]: /nbdserver/ardb/backend.go#L10-L16
[ardb.deduped]: /nbdserver/ardb/deduped.go#L23-L32
[ardb.nondeduped]: /nbdserver/ardb/nondeduped.go#L18-L25
[ardb.tlog]: /nbdserver/ardb/tlog.go#L43-L53
[lba]: /nbdserver/lba
[lba.nilhash]: /nbdserver/lba/hash.go#L15-L16
[vdisk]: https://en.wikipedia.org/wiki/Virtual_disk
[blake2b.hash]: /nbdserver/lba/hash.go#L19-L20