# NBD Server

[NBD][nbd], abbreviation for Network [Block][block] Device, is the lightweight block access protocol used in a Zero-OS cluster to implement [block][block] storage.

A [NBD][nbd] Server actually implements the [NBD][nbd] protocol. For each virtual machine, one [NBD][nbd] Server will be created. Each of these [NBD][nbd] Severs, or [volume driver][voldriver] servers, runs in a separate container, and depends on another container that implements the [TLOG Server][tlogserver]. Each [NBD][nbd] Server can run multiple [vdisks][vdisk] at once.

![Architecture](block-storage-architecture.png)

The failure of one (or more) [vdisk(s)][vdisk] will never make the [NBD][nbd] server crash, and thus impact other [vdisks][vdisk].

Each [vdisk][vdisk] has a type, which can be seen (and is implemented) as a set of properties:

| Disk type | [Redundant][redundant] | [Persistent][persistent] | [Template][template] Support | [Rollback][rollback] |
| --------- | --------- | ---------- | ---------------- | -------- |
| [boot][boot] | yes | yes | yes | yes |
| [db][db] | yes | yes | no | yes |
| [cache][cache] | no | yes | no | no |
| [tmp][tmp] | no | no | no | no |

The [vdisk][vdisk]'s type also defines the type of [storage (2)][storage]. You can learn more about each [storage (2)][storage] type and what [vdisk][vdisk] type use what [storage (2)][storage] type in [the NBD storage docs](storage/storage.md).

Next:
- [Building your NBD Server](building.md).
- [Configuring your NBD Server](config.md).
- [Using your NBD Server](using.md).

[tlogserver]: /docs/tlog/server.md

[nbd]: /docs/glossary.md#nbd
[block]: /docs/glossary.md#block
[vdisk]: /docs/glossary.md#vdisk
[redundant]: /docs/glossary.md#redundant
[persistent]: /docs/glossary.md#persistent
[template]: /docs/glossary.md#template
[rollback]: /docs/glossary.md#rollback
[voldriver]: /docs/glossary.md#volume-driver
[boot]: /docs/glossary.md#boot
[db]: /docs/glossary.md#db
[cache]: /docs/glossary.md#cache
[tmp]: /docs/glossary.md#tmp
[storage]: /docs/glossary.md#storage
