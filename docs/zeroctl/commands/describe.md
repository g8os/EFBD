# zeroctl describe

## snapshot

Describe a snapshot.

A snapshot will be described in JSON format and written to the STDOUT.
The printed JSON object can have following properties:

+ `snapshotID`: the identifier of the snapshot, as defined when exporting a vdisk;
+ `blockSize`: the size (in bytes) of each (plain, decompressed and deduped) block that make up the snapshot's data;
+ `size`: the total (plain and decompressed) data size (in bytes) of the snapshot, as in `blockSize * blockCount`;
+ `created`: indicates when this snapshot was created (date+time in format RFC3339);
+ `version`: tool version that was used to create this snapshot;
+ `source`: information about the vdisk that was exported to create this snapshot;

Note that the snapshot size does not equal a vdisk's size.
A vdisk's (actual) size is defined by its blocksize and the biggest block index stored for that vdisk.
Meaning that if you have a blocksize of 4096 (bytes) and the biggest block index stored is 1000,
the actual size of your vdisk at that moment will be 4100096 bytes or 4004 KiB.
A vdisk's actual size is computed using the following formula:

    vdiskActualSize = vdiskBlockSize * (maxVdiskBlockIndex+1)

This is different from the (actual) size of a snapshot that we output as part of a snapshot's description.
The size of a snapshot is simply the total size that is used to store the snapshot,
and will almost always be a lot lower than the actual size of the vdisk that would be created by importing this vdisk.
The reason being is that we do not care about the actual spreading of the blocks (in terms of their block index),
when computing that size, and instead only care about the size of each block stored and how many blocks we have stored.
A snapshot's size is computed using the following formula:

    snapshotSize = snapshotBlockSize * snapshotBlockCount

Also note that in both the vdisk size and snapshot size we do not take into account
the metadata as part of the size. This is because the metadata is not important in this context,
as we only care about the actual blocks (content) when transferring between the snapshot and vdisk storage format.
The snapshot size is not important, neither accurate, and is computed on the fly while executing this command.

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
+ `ftp://1.2.3.4:200/root/dir`;
+ `ftp://user@127.0.0.1:200`;
+ `ftp://user:pass@12.30.120.200:3000`;
+ `ftp://user:pass@12.30.120.200:3000/root/dir`;

Alternatively you can also give a local directory path to the `--storage` flag,
to backup to the local file system instead.
This is also the default in case the `--storage` flag is not specified.

When the `--storage` flag contains an FTP storage config and at least one of 
`--tls-server`/`--tls-cert`/`--tls-insecure`/`--tls-ca` flags are given,
FTPS (FTP over SSL) is used instead of a plain FTP connection.
This enables describing backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
```
Usage:
  zeroctl describe snapshot snapshotID [flags]

Flags:
  -c, --compression CompressionType   the compression type to use, options { lz4, xz } (default lz4)
  -h, --help                          help for snapshot
  -k, --key AESCryptoKey              an optional 32 byte fixed-size private key used for encryption when given
      --pretty                        pretty print output when this flag is specified
  -s, --storage StorageConfig         ftp server url or local dir path to read the snapshot's header from (default $HOME/.zero-os/nbd/vdisks)
      --tls-ca string                 optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)
      --tls-cert string               PEM-encoded file containing the TLS Client cert (FTPS will be used when given)
      --tls-insecure                  when given FTP over SSL will be used without cert verification
      --tls-key string                PEM-encoded file containing the private TLS client key
      --tls-server string             certs will be verified when given (required when --tls-insecure is not used)
      
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
  	},
  	"version": "1.1.0-beta-1"
}
```

Just as with the [export][export] and [import][import] commands,
can an encrypted snapshot be described by specifying the `-k`/`--key` (private key) flag.

We can add TLS flags to connect to an FTPS server:

```
$ zerodisk describe snapshot foo -s ftp://1.2.3.4:21 \  
    --tls-server 1.2.3.4 \ 
    --tls-cert sample.cert --tls-key sample.key 
```

[vdisk]: /docs/glossary.md#vdisk
[import]: /docs/zeroctl/commands/import.md#vdisk
[export]: /docs/zeroctl/commands/export.md#vdisk
