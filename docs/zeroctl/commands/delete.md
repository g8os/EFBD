# zeroctl delete

## vdisk

Delete a [vdisk][vdisk].

> WARNING: until [issue #88](https://github.com/zero-os/0-Disk/issues/88) has been resolved,
  only the [metadata (1)][metadata] of [deduped][deduped] [vdisks][vdisk] can be deleted by this command.
  [Nondeduped][nondeduped] [vdisks][vdisk] have no [metadata][metadata], and thus are not affected by this issue.

```
Usage:
  zeroctl delete vdisk vdiskid [flags]

Flags:
      --config SourceConfig   config resource: dialstrings (etcd cluster) or path (yaml file) (default config.yml)
  -h, --help                  help for vdisk

Global Flags:
  -v, --verbose   log available information
```

### Examples

To delete a [vdisk][vdisk] `foo`, we would do:

```
$ zeroctl delete vdisk foo
```


[vdisk]: /docs/glossary.md#vdisk
[metadata]: /docs/glossary.md#metadata
[deduped]: /docs/glossary.md#deduped
[nondeduped]: /docs/glossary.md#nondeduped

[nbdconfig]: /docs/nbd/config.md
