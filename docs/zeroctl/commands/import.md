# zeroctl import

## vdisk

Import a [vdisk][vdisk] from a storage (FTP) server,
where the backup is stored in a secure and efficient manner.

> (!) Remember to use the same (snapshot) name,
crypto (private) key and the compression type,
as you used while exporting the backup in question.
>
> The crypto (private) key has a required fixed length of 32 bytes,
and cannot be all zeroes.

If an error occured during the import process,
blocks might already have been written to the block storage.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.
Deleting the [vdisk][vdisk] in such a scenario will help with this problem.

The FTP information is given as the `--storage` flag,
here are some examples of valid values for that flag:
+ `localhost:22`;
+ `ftp://1.2.3.4:200`;
+ `ftp://user@127.0.0.1:200`;
+ `ftp://user:pass@12.30.120.200:3000`;
+ `ftp://user:pass@12.30.120.200:3000/root/dir`;

Alternatively you can also give a local directory path to the `--storage` flag,
to backup to the local file system instead.
This is also the default in case the `--storage` flag is not specified.

```
Usage:
  zeroctl import vdisk vdiskid cryptoKey snapshotID [flags]

Flags:
  -b, --blocksize int                 the size of the exported (deduped) blocks (default 131072)
  -c, --compression CompressionType   the compression type to use, options { lz4, xz } (default lz4)
      --config SourceConfig           config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
  -f, --force                         when given, delete the vdisk if it already existed
  -h, --help                          help for vdisk
  -j, --jobs int                      the amount of parallel jobs to run (default 4)
  -s, --storage StorageConfig         ftp server url or local dir path to import the backup from (default /Users/glendc/.zero-os/nbd/vdisks)

Global Flags:
  -v, --verbose   log available information
```

More (technical) information about the backup module can be found in [the (nbd) backup documentation](/docs/nbd/backup.md).

### Examples

To import a [vdisk][vdisk] `a`, and thus restore from a backup (snapshot) stored on an FTP server `1.2.3.4:21`:

```
$ zerodisk import vdisk a 01234567890123456789012345678901 mybackup -s ftp://1.2.3.4:21
```

If the vdisk `a` already existed on the ARDB storage cluster specified in the config the `-f` flag can be specified,
to forcefully delete the existing vdisk before restoring the vdisk from the specified backup:

```
$ zerodisk import vdisk a 01234567890123456789012345678901 mybackup -s ftp://1.2.3.4:21 -f
```

If we want to import a snapshot which was compressuing using the the `XZ` compression algorithm we can do:

```
$ zerodisk import vdisk a 01234567890123456789012345678901 mybackup -s ftp://1.2.3.4:21 -cxz
```

If we want to use an [etcd][etcd] cluster for our [vdisk][vdisk] configuration we can do:

```
$ zerodisk import vdisk a 01234567890123456789012345678901 mybackup -s ftp://1.2.3.4:21 -config 1.2.3.4:2000
```

[vdisk]: /docs/glossary.md#vdisk
[etcd]: /docs/glossary.md#etcd
