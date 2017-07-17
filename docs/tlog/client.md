# TLog Client

The TLog client is used to send (sequenced) transactions [logs (3)][log] to the [TLog server][tlogserver].

This client is used by the [TLog storage][tlogstorage] as a technique to make [blocks][block] [redundant][redundant], by [logging (3)][log] all write transactions asynchronously as well as [storing (1)][storage] it. 

You can find the documentation for this module at [godoc.org/github.com/zero-os/0-Disk/tlog/tlogclient](http://godoc.org/github.com/zero-os/0-Disk/tlog/tlogclient).

The code for the TLog client can be found in the [/tlog/tlogclient](/tlog/tlogclient) module.

A complete example can be found in [/tlog/tlogclient/examples/send_tlog/client.go](/tlog/tlogclient/examples/send_tlog/client.go).


[tlogserver]: server.md

[log]: /docs/glossary.md#log
[redundant]: /docs/glossary.md#redundant
[block]: /docs/glossary.md#block
[storage]: /docs/glossary.md#storage

[tlogstorage]: /docs/nbd/storage/tlog.md
