# zeroctl describe

## snapshot

Describe a snapshot.

> (!) Remember to use the same (snapshot) name,
crypto (private) key and the compression type,
as you used while exporting the backup in question.
>
> The crypto (private) key has a required fixed length of 32 bytes.
If the snapshot wasn't encrypted, no key should be given,
giving a key in this scenario will fail the describe.

The FTP information is given using the `--storage` flag,
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
  zeroctl describe snapshot snapshotID [flags]

Flags:
  -c, --compression CompressionType   the compression type to use, options { lz4, xz } (default lz4)
  -h, --help                          help for snapshot
  -k, --key AESCryptoKey              an optional 32 byte fixed-size private key used for encryption when given
      --pretty                        pretty print output when this flag is specified
  -s, --storage StorageConfig         ftp server url or local dir path to read the snapshot's header from (default $HOME/.zero-os/nbd/vdisks)

Global Flags:
  -v, --verbose   log available information
```

More (technical) information about the backup module can be found in [the (nbd) backup documentation](/docs/nbd/backup.md).

### Examples

To describe a [vdisk][vdisk]'s public snapshot `foo` stored on an FTP server `1.2.3.4:21`:

```
$ zerodisk describe snapshot foo -s ftp://1.2.3.4:21
{"snapshotID":"foo","blockSize":131072,"size":14024704,"created":"2017-10-02T22:29:06-05:00","source":{"vdiskID":"foo","blockSize":4096,"size":10737418240}}
```

If this output is meant for a human you can make it easier to read by defining the `--pretty` flag:

```
$ zerodisk describe snapshot foo -s ftp://1.2.3.4:21 --pretty
{
  	"snapshotID": "foo",
  	"blockSize": 131072,
  	"size": 14024704,
  	"created": "2017-10-02T22:29:06-05:00",
  	"source": {
  	  	"vdiskID": "foo",
  	  	"blockSize": 4096,
  	  	"size": 10737418240
  	}
}
```

Just as with the [export][export] and [import][import] commands,
can an encrypted snapshot be described by specifying the `-k`/`--key` (private key) flag.

[vdisk]: /docs/glossary.md#vdisk
[import]: /docs/zeroctl/commands/import.md#vdisk
[export]: /docs/zeroctl/commands/export.md#vdisk