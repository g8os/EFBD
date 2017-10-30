# zeroctl list

## vdisks

List all [vdisks][vdisk] available on the url's [ARDB][ardb].

This command can list vdisks on an entire cluster,
as wel as on a single storage server. Some examples:

    zeroctl vdisk list myCluster
    zeroctl vdisk list localhost:2000
    zeroctl vdisk list 127.0.0.1:16379@5

> WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the [ARDB][ardb] server
  in question, by locking the server down for each operation.

```
Usage:
  zeroctl list vdisks (clusterID|address[@db]) [flags]

Flags:
      --config SourceConfig   config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
  -h, --help     help for vdisks

Global Flags:
  -v, --verbose   log available information
```

### Example

List [vdisks][vdisk] available on `localhost:16379`:

```
$ zeroctl list vdisks localhost:16379
```

List [vdisks][vdisk] available on cluster `foo`:

```
$ zeroctl list vdisks foo
```

## snapshots

List all snapshots available locally or an FTP(S) server.

For each vdisk that is exported to the backup storage,
an identifier will be printed on a newline.
It identifies the snapshot and is same one as choosen at export time.

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

When the `--storage` flag contains an FTP storage config and at least one of 
`--tls-server`/`--tls-cert`/`--tls-insecure`/`--tls-ca` flags are given,
FTPS (FTP over SSL) is used instead of a plain FTP connection.
This enables listing backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
```
Usage:
  zeroctl list snapshots [flags]

Flags:
  -h, --help                    help for snapshots
      --name string             list only snapshots which match the given name (supports regexp)
  -s, --storage storageConfig   ftp server url or local dir path to list the snapshots from (default $HOME/.zero-os/nbd/vdisks)
      --tls-ca string           optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)
      --tls-cert string         PEM-encoded file containing the TLS Client cert (FTPS will be used when given)
      --tls-insecure            when given FTP over SSL will be used without cert verification
      --tls-key string          PEM-encoded file containing the private TLS client key
      --tls-server string       certs will be verified when given (required when --tls-insecure is not used)
      
Global Flags:
  -v, --verbose   log available information
```

More (technical) information about the backup module can be found in [the (nbd) backup documentation](/docs/nbd/backup.md).

### Examples

To list all snapshots stored on an FTP server `1.2.3.4:21`:

```
$ zerodisk list snapshots -s ftp://1.2.3.4:21
lede-1701_default
ovc:ubuntu-1604_default
ubuntu-1604_default
```

We can add TLS flags to connect to an FTPS server:

```
$ zerodisk list snapshots -s ftp://1.2.3.4:21 \  
    --tls-server 1.2.3.4 \ 
    --tls-cert sample.cert --tls-key sample.key 
```

[import]: /docs/zeroctl/commands/import.md#vdisk
[export]: /docs/zeroctl/commands/export.md#vdisk
[vdisk]: /docs/glossary.md#vdisk
[ardb]: /docs/glossary.md#ardb
