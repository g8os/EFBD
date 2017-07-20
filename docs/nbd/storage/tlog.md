# TLog Storage

Both the [boot][boot]- and [db][db] [vdisk][vdisk] types have TLog support. [VDisks][vdisk] of this type can enable it by defining the addres(ses) of (a) TLog servers in the [vdisk][vdisk]'s configuration. When enabled, the TLog [storage (3)][storage] is used on top of their normal [storage (2)][storage] type.

The code for this storage type can be found in [/nbdserver/ardb/tlog.go](/nbdserver/ardb/tlog.go).

You can read the [storage docs](/docs/nbd/storage/storage.md) for more information about other available storage types.

## Storage

The TLog storage delegates the actual storage work to the [deduped][deduped]- or [non-deduped][nondeduped] storage type. It does so by wrapping around it.

![TLog Storage](/docs/assets/nbd_tlog_storage.png)

Rather than defining actual [block][block] storage logic, the TLog storage has another purpose instead. All write (`SET`, `DELETE`, `MERGE`) operations are send asynchrounsouly to the [TLog Server][tlogserver], using the [TLog Client][tlogclient]. By doing so the [vdisk][vdisk] can be [rolled back][rollback] to an earlier [logged (3)][log] in history. Additionally the [Transaction Log][tlog] can also be used to create a new [vdisk][vdisk] using the [logged (3)][log] transactions of a TLog-supported [vdisk][vdisk].

For more information about the data [redundancy][redundant] provided by this storage you can read the [TLog docs][tlog].

## Possible Failures

Any read/write operation will fail if the underlying [storage (2)][storage] fails for any reason.

[Logging (3)][log] transactions will fail if:

+ None of the [TLog servers][tlogserver] specified by the [configured][config] addresses are reachable;
+ An uncaught failure happened in the used [TLog server][tlogserver];
+ The [VDisk][vdisk] was terminated  before all transactions were sent and processed by the [TLog server][tlogserver]; 


[boot]: /docs/glossary.md#boot
[db]: /docs/glossary.md#db
[vdisk]: /docs/glossary.md#vdisk
[storage]: /docs/glossary.md#storage
[block]: /docs/glossary.md#block
[rollback]: /docs/glossary.md#rollback
[log]: /docs/glossary.md#log
[redundant]: /docs/glossary.md#redundant

[config]: /docs/config.md

[deduped]: /docs/nbd/storage/deduped.md
[nondeduped]: /docs/nbd/storage/nondeduped.md

[tlog]: /docs/tlog/tlog.md
[tlogserver]: /docs/tlog/server.md
[tlogclient]: /docs/nbd/storage/client.md
