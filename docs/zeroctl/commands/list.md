# zeroctl list

## vdisks

List all [vdisks][vdisk] available on the url's [ARDB][ardb].

> WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the [ARDB][ardb] server
  in question, by locking the server down for each operation.

```
Usage:
  zeroctl list vdisks storage_url [flags]

Flags:
      --db int   database index to use for the ardb connection (0 by default)
  -h, --help     help for vdisks

Global Flags:
  -v, --verbose   log available information
```

### Example

List [vdisks][vdisk] available on `localhost:16379`:

```
$ zeroctl list vdisks localhost:16379
```


[vdisk]: /docs/glossary.md#vdisk
[ardb]: /docs/glossary.md#ardb
