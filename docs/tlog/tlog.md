# TLog

The Transaction [Log (3)][log] (TLog) module provides a [server][tlogserver], [client][tlogclient] and [player][tlogplayer] (used for [rollbacks][rollback] and [replays][replay]). Its purpose is to make [vdisk][vdisk]'s [data (1)][data] and [metadata (1,2,3)][metadata] [redundant][redundant] by storing all write transactions applied by the user in a seperate [storage (3)][storage] in a secure and efficient manner.

Both the [boot][boot]- and [db][db] [vdisk][vdisk] types have TLog support. [VDisks][vdisk] of this type can enable it by defining a TLog [storage (1)][storage] cluster in [its configuration][tlogconfig].

[0-stor][0-stor] is used to store TLog [data (2)][data] and [metadata (4)][metadata], using the [0-stor-lib][0-stor-lib]. See those repositories if you want more information about how the content is stored and secured.

## Components

* [TLog Server](server.md)
  - server (app) which [logs (3)][log] all incoming transactions
* [TLog Client](client.md)
  - client (lib) used to send transactions to the [TLog server](server.md)
* [TLog Player](player.md)
  - player (lib) used to [restore][restore] transactions


[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[redundant]: /docs/glossary.md#redundant
[vdisk]: /docs/glossary.md#vdisk
[storage]: /docs/glossary.md#storage
[boot]: /docs/glossary.md#boot
[db]: /docs/glossary.md#db
[log]: /docs/glossary.md#log
[restore]: /docs/glossary.md#restore
[rollback]: /docs/glossary.md#rollback
[replay]: /docs/glossary.md#replay

[tlogserver]: server.md
[tlogclient]: client.md
[tlogplayer]: player.md
[tlogconfig]: config.md

[0-stor-lib]: https://github.com/zero-os/0-stor-lib
[0-stor]: https://github.com/zero-os/0-stor
