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

[vdisk]: /docs/glossary.md#vdisk
[ardb]: /docs/glossary.md#ardb
