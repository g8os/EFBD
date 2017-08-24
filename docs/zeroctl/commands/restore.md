# zeroctl restore

## vdisk

[Restore][restore] a [vdisk][vdisk] using (a) given [tlogserver(s)][tlogserver].

```
Usage:
  zeroctl restore vdisk id [flags]

Flags:
      --config string              zeroctl config file (default "config.yml")
      --data-shards int            data shards (K) variable of erasure encoding (default 4)
      --end-timestamp uint         end timestamp in nanosecond(default 0: until the end)
  -h, --help                       help for vdisk
      --nonce string               hex nonce used for encryption (default "37b8e8a308c354048d245f6d")
      --parity-shards int          parity shards (M) variable of erasure encoding (default 2)
      --priv-key string            private key (default "12345678901234567890123456789012")
      --start-timestamp uint       start timestamp in nanosecond(default 0: since beginning)
      --storage-addresses string   comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config

Global Flags:
  -v, --verbose   log available information
```

### Examples

[Restore][restore] a complete [vdisk][vdisk] `a`:

```
$ zeroctl restore vdisk a
```

There are also optional timestamp (in nanosecond) options.

[Restore][restore] [vdisk][vdisk] `a` from timestamp `x` to the end:

```
$ zeroctl restore vdisk a --start-timestamp=x
```

[Restore][restore] [vdisk][vdisk] `a` from the start until timestamp `x`

```
$ zeroctl restore vdisk a --end-timestamp=x
```


[restore]: /docs/glossary.md#restore
[vdisk]: /docs/glossary.md#vdisk

[tlogserver]: /docs/tlog/server.md
