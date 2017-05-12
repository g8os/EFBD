# LBA lookups for deduped vdisks

The data in the ardb's is deduped and stored using the blake2b hash of the content as it's key so there is an LBA translation for every read operation. Doing a lookup for every read towards an external ardb is way too slow so we need some LBA lookup table caching.

The LBA lookup tables are split per 128 entries, let's call them LBA-shards. The datastructure is simply an array containing the hashes on the appropriate index so an LBA-shard has a maximum size of 4096 bytes and can address 512 KiB (if 4KB blocks).

An read operation at offset `o` means:

```
* block index = o/4096                      // blocksize of 4096
* LBA-shard number = block index / 128      // 128 entries per LBA-shard
* Get LBA-shard through LRU cache
* hash = LBA-shard[block index % 128]
* ardbinstance = ardblist[hash[0]]          // 256 ardb's and use the first byte of the hash to find the correct one
* content = ardbinstance.get(hash)
```
