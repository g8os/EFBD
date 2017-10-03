# zeroctl copy

## vdisk

Copy a [vdisk][vdisk] configured in [the config file][nbdconfig].

If no target [storage (1)][storage] cluster is given,
the [storage (1)][storage] cluster configured for the source [vdisk][vdisk]
will also be used for the target [vdisk][vdisk].

If an error occured, the target [vdisk][vdisk] should be considered as non-existent,
even though ([meta][metadata])[data][data] which is already copied is not rolled back.

> NOTE: by design,
  only the [metadata][metadata] of a [deduped][deduped] [vdisk][vdisk] is copied,
  the [data][data] will be copied the first time the [vdisk][vdisk] spins up,
  on the condition that the `templateStorageCluster` has been [configured][nbdconfig].

> WARNING: when copying [nondeduped][nondeduped] [vdisks][vdisk],
  it is currently not supported that the target [vdisk][vdisk]'s data cluster
  has more or less [storage (1)][storage] servers, then the source [vdisk][vdisk]'s [storage (1)][storage] cluster.
  See [issue #206](https://github.com/zero-os/0-Disk/issues/206) for more information.

```
Usage:
  zeroctl copy vdisk source_vdiskid target_vdiskid [target_cluster] [flags]

Flags:
Flags:
      --config SourceConfig   config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
      --data-shards int       data shards (K) variable of erasure encoding (default 4)
      --flush-size int        number of tlog blocks in one flush (default 25)
  -h, --help                  help for vdisk
  -j, --jobs int              the amount of parallel jobs to run the tlog generator (default $NUMBER_OF_CPUS)
      --parity-shards int     parity shards (M) variable of erasure encoding (default 2)
      --priv-key string       private key (default "12345678901234567890123456789012")
      --same                  enable flag to force copy within the same nbd servers

Global Flags:
  -v, --verbose   log available information
```

### Examples

To copy `vdiskA` as a new [vdisk][vdisk] (`vdiskB`) on the _same_ [storage (1)][storage] cluster (`clusterA`), I would do:

```
$ zeroctl copy vdisk vdiskA vdiskB
```

Which would be the same as the more explicit version:

```
$ zeroctl copy vdisk vdiskA vdiskB clusterA --config config.yml
```

To copy `vdiskA` as a new [vdisk][vdisk] (`vdiskA`) on a _different_ [storage (1)][storage] cluster (`clusterB`), I would do:

```
$ zeroctl copy vdisk vdiskA vdiskA clusterB
```

The following command would be illegal, and abort with an error:

```
$ zeroctl copy vdisk vdiskA vdiskA
```


[vdisk]: /docs/glossary.md#vdisk
[metadata]: /docs/glossary.md#metadata
[data]: /docs/glossary.md#data
[storage]: /docs/glossary.md#storage
[deduped]: /docs/glossary.md#deduped
[nondeduped]: /docs/glossary.md#nondeduped

[nbdconfig]: /docs/nbd/config.md