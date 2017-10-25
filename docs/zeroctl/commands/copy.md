# zeroctl copy

## vdisk

Copy a source [vdisk][vdisk] as a new target [vdisk][vdisk],
both configured in [the config][nbdconfig].

If an error occured, the target [vdisk][vdisk] should be considered as non-existent,
even though ([meta][metadata])[data][data] which is already copied is not rolled back.

> NOTE: by design,
  only the [metadata][metadata] of a [deduped][deduped] [vdisk][vdisk] is copied,
  the [data][data] will be copied the first time the [vdisk][vdisk] spins up,
  on the condition that the `templateStorageCluster` has been [configured][nbdconfig].

> NOTE: the storage types and block sizes of source and target [vdisk][vdisk]
  need to be equal, else an error is returned.

```
Usage:
  zeroctl copy vdisk source_vdiskid target_vdiskid [flags]

Flags:
      --config SourceConfig   config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
      --flush-size int        number of tlog blocks in one flush (default 25)
  -f, --force                 when given, delete the target vdisk if it already existed
  -h, --help                  help for vdisk
  -j, --jobs int              the amount of parallel jobs to run the tlog generator (default 4)
      --priv-key string       private key (default "12345678901234567890123456789012")
      --same                  enable flag to force copy within the same nbd servers

Global Flags:
  -v, --verbose   log available information
```

### Examples

To copy `vdiskA` as a new [vdisk][vdisk] (`vdiskB`), I would do:

```
$ zeroctl copy vdisk vdiskA vdiskB
```

Which would be the same as the more explicit version:

```
$ zeroctl copy vdisk vdiskA vdiskB --config config.yml
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