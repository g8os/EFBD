# zeroctl delete

## vdisks

Delete one, multiple or all [vdisks][vdisk].

When no vdiskids are specified,
all [vdisks][vdisk] listed in [the config file][nbdconfig] will be deleted.

> WARNING: until [issue #88](https://github.com/zero-os/0-Disk/issues/88) has been resolved,
  only the [metadata (1)][metadata] of [deduped][deduped] [vdisks][vdisk] can be deleted by this command.
  [Nondeduped][nondeduped] [vdisks][vdisk] have no [metadata][metadata], and thus are not affected by this issue.

```
Usage:
  zeroctl delete vdisks [vdiskid...] [flags]

Flags:
      --config string   zeroctl config file (default "config.yml")
  -f, --force           when enabled non-fatal errors are logged instead of aborting the command
  -h, --help            help for vdisks

Global Flags:
  -v, --verbose   log available information
```

### Examples

To delete all [vdisks][vdisk] listed in [the config file][nbdconfig]:

```
$ zeroctl delete vdisks
```

Which is the less explicit version of:

```
$ zeroctl delete vdisks --config config.yml
```

To delete only 1 (or more) [vdisks][vdisk], rather than all, we can specify their identifiers:

```
$ zeroctl delete vdisks vdiskC --config.yml
```

With this knowledge we can write the first delete example even more explicit:

```
$ zeroctl delete vdisks vdiskA vdiskC --config.yml
```

The following would succeed for the found [vdisk][vdisk], but log an error for the other [vdisk][vdisk] as that one can't be found:

```
$ zeroctl delete vdisks foo vdiskA # vdiskA will be deleted correctly, even though foo doesn't exist
```


[vdisk]: /docs/glossary.md#vdisk
[metadata]: /docs/glossary.md#metadata
[deduped]: /docs/glossary.md#deduped
[nondeduped]: /docs/glossary.md#nondeduped

[nbdconfig]: /docs/nbd/config.md
