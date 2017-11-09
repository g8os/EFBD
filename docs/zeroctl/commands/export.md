# zeroctl export

## vdisk

Export a [vdisk][vdisk] to a storage (FTP) server,
in a secure and efficient manner.

> (!) Remember to keep note of the used (snapshot) name,
crypto (private) key and the compression type,
as you will need the same information when importing the exported backup.
>
> The crypto (private) key has a required fixed length of 32 bytes.
If no key is given, the compressed snapshot will not be encrypted.

If an error occured during the export process,
deduped blocks might already have been written to the FTP server.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.

If the snapshotID is not given,
one will be generated automatically using the "<vdiskID>_epoch" format.
The used snapshotID will be printed in the STDOUT in case
no (fatal) error occured, at the end of the command's lifetime.

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

When the --force flag is given,
a deduped map will be overwritten if it already existed,
AND if it couldn't be loaded, due to being corrupt or encrypted/compressed,
using a different private key or compression type, than the one(s) used right now.

By default LZ4 compression is used, which is the fastest of the supported compression algorithms.
XZ compression can be used, which has a better compression ratio but slows down the export of a vdisk.

When the `--storage` flag contains an FTP storage config and at least one of 
`--tls-server`/`--tls-cert`/`--tls-insecure`/`--tls-ca` flags are given,
FTPS (FTP over SSL) is used instead of a plain FTP connection.
This enables exporting backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.

```
Usage:
  zeroctl export vdisk vdiskid [snapshotID] [flags]

Flags:
  -b, --blocksize int                 the size of the exported (deduped) blocks (default 131072)
  -c, --compression CompressionType   the compression type to use, options { lz4, xz } (default lz4)
      --config SourceConfig           config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
  -f, --force                         when given, overwrite a deduped map if it can't be loaded
  -h, --help                          help for vdisk
  -j, --jobs int                      the amount of parallel jobs to run (default $NUMBER_OF_CPUS)
  -k, --key AESCryptoKey              an optional 32 byte fixed-size private key used for encryption when given
  -s, --storage StorageConfig         ftp server url or local dir path to export the backup to (default $HOME/.zero-os/nbd/vdisks)
      --tls-ca string                 optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)
      --tls-cert string               PEM-encoded file containing the TLS Client cert (FTPS will be used when given)
      --tls-insecure                  when given FTP over SSL will be used without cert verification
      --tls-key string                PEM-encoded file containing the private TLS client key
      --tls-server string             certs will be verified when given (required when --tls-insecure is not used)s

Global Flags:
  -v, --verbose   log available information
```

More (technical) information about the backup module can be found in [the (nbd) backup documentation](/docs/nbd/backup.md).

### Examples

To export a [vdisk][vdisk] `a`, and thus make a backup (snapshot) on an FTP server `1.2.3.4:21`:

```
$ zerodisk export vdisk a -k 01234567890123456789012345678901 -s ftp://1.2.3.4:21
```

If we want to define our own snapshot id for the backup we could instead do:

```
$ zerodisk export vdisk a mybackup -k 01234567890123456789012345678901 -s ftp://1.2.3.4:21
```

If we want to use the `XZ` compression algorithm instead of the `LZ4` compression algorithm we can do:

```
$ zerodisk export vdisk a -k 01234567890123456789012345678901 -s ftp://1.2.3.4:21 -cxz
```

If we want to use an [etcd][etcd] cluster for our [vdisk][vdisk] configuration we can do:

```
$ zerodisk export vdisk a -k 01234567890123456789012345678901 -s ftp://1.2.3.4:21 --config 1.2.3.4:2000
```

If we want to create a public backup (without encryption) we can omit the `-k` flag:

```
$ zerodisk export vdisk a -s ftp://1.2.3.4:21 --config 1.2.3.4:2000
```

We can add TLS flags to connect to an FTPS server:

```
$ zerodisk export vdisk a -s ftp://1.2.3.4:21 \
     --tls-server 1.2.3.4 \
     --tls-cert sample.cert --tls-key sample.key 
```

[vdisk]: /docs/glossary.md#vdisk
[etcd]: /docs/glossary.md#etcd
