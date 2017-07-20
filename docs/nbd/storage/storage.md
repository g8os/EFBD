# Storage

The storage types define how a [persistent][persistent] [vdisk][vdisk]'s [data (1)][data] and [metadata (1,2,3)][metadata] is stored in an [ARDB cluster][ardb]. On top of that it also coordinate with any other optional services as the defined by the specific storage type.

![NBD Storage Overview](/docs/assets/nbd_storage_overview.png)

The [NBD Backend][backend] mainly converts the position in bytes into a [block index (2)][index], and delegates most of the actual storage work to the underlying storage type (blue `storage` rectangle in the graph above).

## Storage Types

### Deduped Storage

It stores any given [block][block] only once. Meaning it has no duplicated [blocks][block], hence its name. All [blocks][block] are identified by their [hash][hash], and an [LBA][lba] scheme is used to map each [block index (2)][index] to the correct [block][block] [hash][hash].

![Deduped Storage](/docs/assets/nbd_deduped_storage.png)

See the [deduped storage docs](/docs/nbd/storage/deduped.md) for more information.

The code for this storage type can be found in [/nbdserver/ardb/deduped.go](/nbdserver/ardb/deduped.go).

### Non Deduped Storage

The simplest storage type of them all. It stores all [blocks][block] directly mapped to the [block index (2)][index] it belongs to. It has no [metadata][metadata].

![Non Deduped Storage](/docs/assets/nbd_nondeduped_storage.png)

See the [non-deduped storage docs](/docs/nbd/storage/nondeduped.md) for more information.

The code for this storage type can be found in [/nbdserver/ardb/nondeduped.go](/nbdserver/ardb/nondeduped.go).

### Semi Deduped Storage

It is a hybrid storage, combining the [non-deduped](#non-deduped-storage)- and [deduped](#deduped-storage) storage. The [non-deduped](#non-deduped-storage) [blocks][block] are writiable and filled by the user, while the [deduped](#deduped-storage) [blocks][block] is read-only and filled by the _required_ [template][template] [storage (1) cluster][storage].

![Semi Deduped Storage](/docs/assets/nbd_semideduped_storage.png)

See the [semi-deduped storage docs](/docs/nbd/storage/semideduped.md) for more information.

The code for this storage type can be found in [/nbdserver/ardb/semideduped.go](/nbdserver/ardb/semideduped.go).

### TLog Storage

It delegates the actual storage work to the [deduped](#deduped-storage)- or [non-deduped](#non-deduped-storage) storage type. It essentially works as an interceptor, intercepting any write transactions and sending them asynchrounsouly to the [TLog Server][tlogserver], using the [TLog Client][tlogclient].

![TLog Storage](/docs/assets/nbd_tlog_storage.png)

See the [TLog storage docs](/docs/nbd/storage/tlog.md) for more information.

The code for this storage type can be found in [/nbdserver/ardb/tlog.go](/nbdserver/ardb/tlog.go).

[backend]: /docs/glossary.md#backend
[persistent]: /docs/glossary.md#persistent
[vdisk]: /docs/glossary.md#vdisk
[template]: /docs/glossary.md#template
[ardb]: /docs/glossary.md#ardb
[storage]: /docs/glossary.md#storage
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[index]: /docs/glossary.md#index
[lba]: /docs/glossary.md#lba
[block]: /docs/glossary.md#block
[hash]: /docs/glossary.md#hash

[tlogserver]: /docs/tlog/server.md
[tlogclient]: /docs/tlog/client.md

