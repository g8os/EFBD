# zeroctl import

## vdisk

Import a [vdisk][vdisk] from a storage (FTP) server,
where the backup is stored in a secure and efficient manner.
Tlog data will be generated if the vdisk has configured tlog cluster.

> (!) Remember to use the same (snapshot) name,
crypto (private) key and the compression type,
as you used while exporting the backup in question.
>
> The crypto (private) key has a required fixed length of 32 bytes.
If the snapshot wasn't encrypted, no key should be given,
giving a key in this scenario will fail the import.

If an error occured during the import process,
blocks might already have been written to the block storage.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.
Deleting the [vdisk][vdisk] in such a scenario will help with this problem.

The FTP information is given as the `--storage` flag,
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
This enables importing backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
```
Usage:
  zeroctl import vdisk vdiskid snapshotID [flags]

Flags:
  -c, --compression CompressionType   the compression type to use, options { lz4, xz } (default lz4)
      --config SourceConfig           config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
      --flush-size int                number of tlog blocks in one flush (default 25)
  -f, --force                         when given, delete the vdisk if it already existed
  -h, --help                          help for vdisk
  -j, --jobs int                      the amount of parallel jobs to run (default $NUMBER_OF_CPUS)
  -k, --key AESCryptoKey              an optional 32 byte fixed-size private key used for decryption when given
  -s, --storage storageConfig         ftp server url or local dir path to import the backup from (default $HOME/.zero-os/nbd/vdisks)
      --tlog-priv-key string          tlog private key (default "12345678901234567890123456789012")
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

To import a [vdisk][vdisk] `a`, and thus restore from a backup (snapshot) stored on an FTP server `1.2.3.4:21`:

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21 -k 01234567890123456789012345678901
```

If the vdisk `a` already existed on the ARDB storage cluster specified in the config the `-f` flag can be specified,
to forcefully delete the existing vdisk before restoring the vdisk from the specified backup:

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21 -f -k 01234567890123456789012345678901
```

If we want to import a snapshot which was compressuing using the the `XZ` compression algorithm we can do:

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21 -cxz -k 01234567890123456789012345678901
```

If we want to use an [etcd][etcd] cluster for our [vdisk][vdisk] configuration we can do:

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21 --config 1.2.3.4:2000 -k 01234567890123456789012345678901
```

If we want to import a public backup we can simply omit the `-k` flag as we no longer have a private key (or encryption):

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21 --config 1.2.3.4:2000
```

We can add TLS flags to connect to an FTPS server:

```
$ zerodisk import vdisk a mybackup -s ftp://1.2.3.4:21  \
    --tls-server 1.2.3.4 \
    --tls-cert sample.cert --tls-key sample.key
```

[vdisk]: /docs/glossary.md#vdisk
[etcd]: /docs/glossary.md#etcd
