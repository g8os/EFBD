# Tools

## Milestone 6: Key Rename

This tool is meant to apply the ([meta][metadata])[data][data] key renaming, due to the fact that redis hashmap keys are prefixed since milstone 6, while they weren't prefixed before. The reason they are prefixed now is to avoid name colissions between [nondeduped][nondeduped] [data][data] and [deduped][deduped] ([LBA][lba]) [metadata][metadata]. Such name colissions were unavoidable when using the [semideduped][semideduped] storage, as uses both [nondeduped][nondeduped] and [deduped][deduped] storage under the hood.

For more information about how all ([meta][metadata])[data][data] is stored you can read the [storage docs][storage].

Executing this tool can take several minutes(!) to finish, depending on the (total) amount of keys available in the given ([ARDB][ardb]) server(s).

> (!!!) Warning: If the `-apply` flag is specified, or you give permission manually, your keys will be altered, with no rollback if something goes wrong in the process. Use this command with precaution! (it is guaranteed however that all renaming operations on a single server are either all applied, or not applied at all)

> (!!!) Warning: Currently this tool will rename all (filtered) legacy hashmaps found on a given redis server, even those that do not belong to the given [vdisk][vdisk] type. Please open a feature request in case you require the migration of ([meta][metadata])[data][data] on servers which have [vdisks][vdisk] of multiple types. For example servers which have both [db][db]- and [boot][boot] [vdisks][vdisk] stored. You have been warned!

### Usage

```
Usage: key_rename_milestone_6 [flags] storage_address...
  -apply
        when specified, apply changes
        without asking first  (default: false)
  -regexp string
        an optional regex which allows you to to process
        only the vdisks which ID match the given (re2) regexp
        (syntax docs: https://github.com/google/re2/wiki/Syntax)
        (disabled by default)
  -type value
        define the type of vdisk you wish to rename
        relevant data for,
        options: boot, db, cache, tmp (default: boot)

At least one storage address has to be given!
```

You can simply run the tool using:

```
$ go run tools/key_rename_milestone_6.go --help
```

Or you can first build it, and than use it:

```
$ go build tools/key_rename_milestone_6.go
./key_rename_milestone_6 --help
```

### Examples

Preview the renaming of all [boot][boot] [vdisks][vdisk]' hashmaps on `localhost:16379`:

```
$ key_rename_milestone_6 localhost:16379 

scanning ardb at localhost:16379@0 for keys...
leed -> lba:leed (preview)
ubuntu -> lba:ubuntu (preview)

2 key(s) can be renamed...
rename the 2 key(s) listed above [y/n]:
```

The output of the above command will show you all the renaming that will take place. If the listed keys are what you would more or less expect, you can apply the renaming by typing "`y`" and hitting _Enter_:

```
rename the 2 key(s) listed above [y/n]: y

renaming 2 key(s)...
leed -> lba:leed
ubuntu -> lba:ubuntu
```

Which will once again list the keys which got renamed. If the above command exists with a non-0 code, it means that no renaming was applied due to an error which will be logged in the `STDERR`. If you answer "`n`" instead of "`y`", no renaming will take place for that server (which is the safe thing to do in case you see a key you didn't expect to be there).

If you have ([meta][metadata])[data][data] spread across multiple [storage (1)][storage] servers, you can give them all at once as follows:

```
$ key_rename_milestone_6 localhost:200 localhost:300
scanning ardb at localhost:200@0 for keys...
leed:5 -> lba:leed:5 (preview)

1 key(s) can be renamed...
rename the 1 key(s) listed above [y/n]:
```

Again, if you're happy with the output, you can apply the renaming by typing "`y`" and hitting _Enter_:

```
rename the 1 key(s) listed above [y/n]: y

renaming 1 key(s)...
leed:5 -> lba:leed:5
```

Then it will scan the next server, and ask again for permission, to which we can answer "`y`" once again (if we want of course):

```
scanning ardb at localhost:300@0 for keys...
leed:9 -> lba:leed:9 (preview)
ubuntu:19 -> lba:ubuntu:19 (preview)
mini -> lba:mini (preview)

3 key(s) can be renamed...
rename the 3 key(s) listed above [y/n]: y

renaming 3 key(s)...
leed:9 -> lba:leed:9
ubuntu:19 -> lba:ubuntu:19
mini -> lba:mini
```

If a non-0 code gets returned, it means that the renaming didn't got applied due to an error in at least 1 [storage][storage] server. For each of these failed servers there will be 1 log message explaining the error.

It can be assumed that if a server did not return an error, that all its relevant keys got renamed successfully. 

In case you love to gamble, you could also auto apply all renaming by given the `-apply` flag. This way no permission will be asked prior to any renaming. Thus the first example would become:

```
$ key_rename_milestone_6 localhost:16379 

scanning ardb at localhost:16379@0 for keys...
leed -> lba:leed (preview)
ubuntu -> lba:ubuntu (preview)

2 key(s) can be renamed...

renaming 2 key(s)...
leed -> lba:leed
ubuntu -> lba:ubuntu
```

If all vdisks on a server that you want to migrate follow a pattern in their ids you could also filter define an extra filter, as to ensure that only the [vdisks][vdisk] whose id matches your (regexp) filter, will be migrated. Here is how you could do that in case all your vdisks are prefixed with "`template:`":

```
$ key_rename_milestone_6 -regexp '^template:' localhost:16379
```

If you want to migrate the [data][data] of [db][db] [vdisks][vdisk] than you can do that using the following command:

```
$ key_rename_milestone_6 -type db localhost:16379
```

Note that this will also rename the [data][data] of [cache][cache]- and [tmp][tmp] vdisks if present, as they share the same underlying [storage][storage] type ([nondeduped][nondeduped]).

[storage]: /docs/glossary.md#storage
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#netadata
[nondeduped]: /docs/glossary.md#nondeduped
[semideduped]: /docs/glossary.md#semideduped
[deduped]: /docs/glossary.md#deduped
[lba]: /docs/glossary.md#lba
[ardb]: /docs/glossary.md#ardb
[vdisk]: /docs/glossary.md#vdisk
[boot]: /docs/glossary.md#boot
[db]: /docs/glossary.md#db
[cache]: /docs/glossary.md#cache
[tmp]: /docs/glossary.md#tmp

[storage]: /docs/nbd/storage/storage.md
