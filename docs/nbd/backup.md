# Backup

The backup module allows you to [export](#export) a [snapshot][snapshot] of a [vdisk][vdisk] onto a storage server in a secure (encrypted) and efficient (deduped) manner. In production and for most purposes the storage server is assumed to be an FTP server. This [snapshot][snapshot] can than be restored (see: [import](#import)) at a later time as a new [vdisk][vdisk] or to overwrite an existing [vdisk][vdisk].

All blocks stored for a given [snapshot][snapshot] are deduped, and the deduped map is stored in the `/backups` subdir, mapping all (block) indices to the deduped block's hashes. The deduped map as well as the deduped blocks are compressed AND encrypted. The compression happens first and can be done using either the default faster and safer [LZ4][lz4] algorithm, or the slower but stronger [XZ][xz] compression algorithm. Once the content is compressed it is encrypted using the [AES stream cipher][aes] algorithm in [GCM mode][gcm] using the user-configured private (32 byte) key. When [importing](#import) a [snapshot][snapshot] decryption happens first, and decompression second, the reverse process of [exporting](#export).

This document explains how backups are created and used, giving a lot of detailed and technical information. If you are simply interested in creating a backup (snapshot) please refer to zeroctl's [export (vdisk) command][export]. If you already have a backup (snapshot) available, and wish to just restore it as a (new) [vdisk][vdisk], please refer to zeroctl's [import (vdisk) command][import].

For more information about the actual Golang implementation of this backup module, and how to use its public API, you can read the [backup Godocs information][backupGodocs].

## Index

1. [Deduped Map](#deduped-map): the metadata which identifies a snapshot and all its deduped blocks;
2. [Storage Types](#storage-types): the different storage types and how they relate;
3. [Block Sizes](#block-sizes): the different block sizes and how they relate;
    1. [Inflation](#inflation): going from smaller to bigger blocks;
    2. [Deflation](#deflation): going from bigger to smaller blocks;
4. [Storage](#storage): information on how a snapshot is stored;
5. [Export](#export): how a snapshot (backup) is created from a [vdisk][vdisk];
6. [Import](#import): how a [vdisk](#vdisk) is created from a snapshot (backup);

## Deduped Map

All blocks of a [vdisk][vdisk]'s [snapshot][snapshot] are deduped. This deduplication is done using the [hash][hash] of the compressed+encrypted block. Meaning that if you have multiple blocks (in one or multiple [snapshots][snapshot]) which are compressed using the same compression algorithm and encrypted using the same private key, will only be stored once.

The deduped map is encoded using the [bencoding][bencode] encoding format. It is choosen for its simplicity and efficiency. The encoded version of the deduped map is also compressed and encrypted before writing it to the storage server, using the same compression algorithm and private key used for the compression and encryption of the (deduped) blocks.

![backup deduped map](/docs/assets/backup_deduped_map.png)

As the content is stored and referenced using its hash, and the hash is stored in an encrypted deduped map, the content is also guaranteed to stay untouched and correctly linked to the block index it was assigned to at serialization time.

## Storage Types

[Vdisks][vdisk] can be [deduped][dedupedVdisk], [non-deduped][nondedupedVdisk] and even [semi-deduped][semidedupedVdisk]. However, no matter the case, all snapshots are stored in a universal way. In fact, you can't even deduce the original vdisk type, just by looking at a snapshots data (even if you had the private key). This has the consequential advantage that you can import a snapshot into any (new) [vdisk][vdisk], regardless of its [storage type][nbdStorageDocs], and regardless of the [storage type][nbdStorageDocs] of the original [vdisk][vdisk] which acted as the source of the snapshot in question.

Read the [storage section](#storage) to learn more about how backups (snapshots) are stored.

More information about [vdisk][vdisk] storage types can be found in the [nbd storage docs][nbdStorageDocs].

## Block Sizes

[Vdisk][vdisk] block sizes are configured for each [vdisk][vdisk]. Even though they have no default, [deduped vdisks][dedupedVdisk] often have a block size of 4 KiB. The block size of a snapshot does however not have any relationship with its vdisk's block size. Despite this disconnect they do have the same validation requirements. A block size needs to be at least 512 bytes and has to be a power of two.

This means that you can go from any valid [vdisk][vdisk] block size to any valid snapshot block size. Similar to the [storage type](#storage-types), it is impossible to tell from a snapshot's data what the original [vdisk][vdisk]'s block size was. Meaning that you can import a snapshot into any (new) [vdisk][vdisk], regardless of its block size and [storage type](#storage-types).

When comparing a [vdisk][vdisk]'s and snapshot's block size, we can conclude that there are only 3 possible scenarios:

1. The [vdisk][vdisk] block size is smaller then the snapshot's block size;
2. The [vdisk][vdisk] block size is equal to the snapshot's block size;
3. The [vdisk][vdisk] block size is greater then the snapshot's block size;

Note that these three scenarios are possible for both the [import](#import) and [export](#export) phase. Scenario (2) is the simplest scenario. Here the block size of both the [vdisk][vdisk] and the snapshot are equal, as we can simply write the blocks from one to the other. But what to do when the block sizes are different?

### Inflation

When exporting [deduped vdisks][dedupedVdisk] the most common scenario will be what we call inflation. Meaning that the source's block size is smaller than that of the destination. And thus it is the most common scenario as [deduped vdisks][dedupedVdisk] are usually stored in blocks of 4 KiB and the default snapshot's block size is 128 KiB. However all of this can be configured to be different, and thus this should be considered more of an example than a rule.

![inflation of blocks](/docs/assets/backup_block_inflation.png)

When we talk about inflation the context of a backup, we mean that we have to form a bigger (destination) block using smaller (source) blocks. All blocks are aligned based on their block size. Meaning that for a storage with a block size of two, each second byte would mark the index and start of a new block. With that knowledge in mind and referring to the image above we can see there are three possible scenarios:

1. All source blocks for a given destination block are nil/empty, in which case no destination block will exist for those source blocks;
2. Some source blocks for a given destination block are filled, resulting in a partially fill destination block;
3. All source blocks for a given destination block are filled, in which case the destination block is partially or completely filled;

There are a couple of scenarios where things get trickier. One scenario is due to the fact that blocks are often non-sequential, meaning we have to potentially zero-fill such gaps. This and other scenarios are however outside the scope of this documentation. You can learn more about all of this by reading through the inline-documented `InflationBlockFetcher` implementation in [/nbd/ardb/backup/backup.go][backupCode].

### Deflation

When importing a snapshot into a (new) [deduped vdisk][dedupedVdisk] the most common scenario will be what we call deflation. For simplicity sake we can see it as the reverse process of [inflation](#inflation). Summarized it means that the source's block size is bigger then that of the destination. And thus it is the most common scenario as [deduped vdisks][dedupedVdisk] are usually stored in blocks of 4 KiB and the default snapshot's block size is 128 KiB. However all of this can be configured to be different, and thus this should be considered as an example, rather than a rule.

![deflation of blocks](/docs/assets/backup_block_deflation.png)

When talking about deflation, we are talking about the scenario where we slice a big (source) block into smaller (destination) blocks. As we know from the [Block Sizes section](#block-sizes), all block sizes have to be a power of two. This makes cutting a big block into smaller blocks really easy, as during deflation, we'll never have a destination block which contains data from multiple source blocks. With that knowledge in mind and referring to the image above we can see that there are three possible scenarios:

1. A source block is nil/empty, meaning that there are no destination blocks to be created for this source block;
2. A source block is partially filled, in which case only the source blocks which actually contain (some) data need to be created;
3. A source block is has data in each (destination block) slice, meaning that all potential destination blocks will in fact be created;

Please read through the inline-documented `DeflationBlockFetcher` implementation in [/nbd/ardb/backup/backup.go][backupCode], should you feel curious about the actual implementation or potential edge cases for the scenarios mentioned above.

## Storage

In production and for most use cases the [snapshots][snapshot] are stored onto a given FTP server. A [snapshot][snapshot] consists out of (deduped) block data, and a [deduped map](#deduped-map). The [deduped map](#deduped-map) links each block index with that block's hash.

The [deduped map](#deduped-map) is stored using the [snapshot][snapshot]'s identifier as its name under the `backups/` subdir of the root directory. By default this is the [vdisk][vdisk]'s identifier and epoch of creation, but this can be configured during creation to be a custom value instead.

All deduped blocks are stored under `/XX/YY/ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ` of the root directory. Thus all blocks are stored in a 2-layer deep directory structure, where `XX` are the first 2 bytes of the block's [hash][hash] and the name of the first directory. `YY` represents the 3rd and 4th bytes of the block's [hash][hash]. `ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ`, the last 28 bytes of the block's [hash][hash] is the name of the block itself.

```
├── 4FE1
│   └── BD63
│       └── 707C57D7D6BE98C342C72150968F25289D0F5BC8B130791E2BA10EAD
├── 6DED
│   └── 8C3A
│       └── 70EA10A1E8C9348B16F4DDE10FD74E7927360C25C11FC3AB60006196
├── 8280
│   └── C093
│       └── 91058E49C373C7A7F6E05E7AD96A9551F35271EAF59FA721BBCDED39
├── 9DDB
│   └── DC58
│       └── 75312CFE5CDFB6BFCA06ACBB17B0DF58F4D7A5F26E6EC7A0C7A43BE4
├── C798
│   └── F788
│       └── 27CB1A2C245301B6B89A288917FF78193C29D34E9F63DCCB507F647B
├── EB9A
│   └── CECC
│       └── 05AA5748B4B4E923E3875AF8DCE67E501E904E3A084EF7AB764C71A5
├── F2D4
│   └── 6556
│       └── 91233D3B9C4BE7599C096886888663F0E6CA16EC16C9A4D64ED21F04
└── backups
    └── a_1503675086
```

Thus a snapshot (backup) is essentially a collection of many deduped blocks, and one deduped map. The tree example above should give you an idea of how a snapshot will be stored on the server. Note that the example above is using a very tiny [vdisk][vdisk]. Most likely you'll have a lot more deduped block files though. Increasing the snapshot's [block size](#block-sizes) will result in less files, but it will also make the de-duplication less effective. The file count is also not a good metric. A better metric is the total size of all blocks for a snapshot. And for that an effective de-duplication will help you a lot.

## Export

The backup module provides a global `Export` function which allows you to export a given [vdisk][vdisk]. While it is possible to do so via the public module API, it is instead usually done using the [zeroctl tool][zeroctl]. It should be noted that a [vdisk][vdisk] should first be unmounted before being exported. While there is no protection against this, failing to do so will result in unexpected behavior.

![backup export](/docs/assets/backup_overview_export.png)

The export process is straightforward and can be summarized as follows:

1. All block indices are collected from the ARDB cluster, ordered from smallest to biggest;
2. The deduped map is loaded if it already existed, and newly created if it didn't;
3. All content is written in a parallel and streamlined fashion:
    1. All blocks are read in parallel from the ARDB storage;
    2. The blocks are ordered in a single goroutine and sized to the correct export block size;
    3. The newly sized blocks are all compressed, encrypted and stored in parallel;
    4. Link the block index with the block hash in the deduped map, unless it is already set, in which case false is returned;
    5. If the block is however new, it will now be stored on the (FTP) Storage Server;
4. Store the deduped map on the (FTP) Storage Server;

Check out [the zeroctl export command documentation][export] for more information on how to export a [vdisk][vdisk] yourself.

Please read through the inline-documented export code at "[/nbd/ardb/backup/export.go](/nbd/ardb/backup/export.go)" for more information and to see how it's actually implemented in detail.

## Import

The backup module provides a global `Import` function which allows you to import a given [vdisk][vdisk]'s [snapshot][snapshot], earlier created by [exporting](#export) a [vdisk][vdisk]. While it is possible to do so via the public module API, it is instead usually done using the [zeroctl tool][zeroctl]. It should be noted that a [vdisk][vdisk] cannot exist already before it can be imported. However if it it does exist it can be forcefully deleted while it's at it, as an option for the user.

![backup import](/docs/assets/backup_overview_import.png)

The import process is straightforward and can be summarized as follows:

1. The [deduped map](#deduped-map) is loaded from the server, and all its hash and index mappings are collected;
2. All content is read and optionally stored in a parallel and streamlined fashion:
    1. All deduped (snapshot) blocks are read in parallel from the given storage (FTP) server;
    2. If the read block is correct and untouched, it is decrypted and decompressed, to send for storage into the ARDB cluster;
    3. The blocks are ordered in a single goroutine and sized to the correct import block size;
    4. The newly sized blocks are all stored in the ARDB storage cluster in a parallel and streamlined fashion;

Check out [the zeroctl import command documentation][import] for more information on how to import a [vdisk][vdisk] yourself.

Please read through the inline-documented import code at "[/nbd/ardb/backup/import.go](/nbd/ardb/backup/import.go)" for more information and to see how it's actually implemented in detail.

[vdisk]: /docs/glossary.md#vdisk
[snapshot]: /docs/glossary.md#snapshot
[hash]: /docs/glossary.md#hash
[dedupedVdisk]: /docs/glossary.md#deduped
[nondedupedVdisk]: /docs/glossary.md#nondeduped
[semidedupedVdisk]: /docs/glossary.md#semideduped

[nbdStorageDocs]: /docs/nbd/storage/storage.go
[backupCode]: /nbd/ardb/backup/backup.go

[lz4]: https://godoc.org/github.com/pierrec/lz4
[xz]: https://godoc.org/github.com/ulikunitz/xz
[aes]: https://godoc.org/crypto/aes
[gcm]: https://godoc.org/crypto/cipher#NewGCM
[bencode]: https://godoc.org/github.com/zeebo/bencode

[zeroctl]: /docs/zeroctl/zeroctl.md
[export]: /docs/zeroctl/commands/export.md
[import]: /docs/zeroctl/commands/import.md

[backupGodocs]: https://godoc.org/github.com/zero-os/0-Disk/nbd/ardb/backup