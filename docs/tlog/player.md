# TLog Player

The [TLog][tlog] player can read the [logged (3) transactions][log] and use it to [restore][restore] a [vdisk][vdisk] by writing it directly to the [NBD backend][backend]. This player is used by the [zeroctl][zeroctl]'s [restore command][restorecmd] for example.

![TLog Player overview](/docs/assets/tlog_player_overview.png)

You can find the documentation for this module at [godoc.org/github.com/zero-os/0-Disk/tlog/tlogclient/player](http://godoc.org/github.com/zero-os/0-Disk/tlog/tlogclient/player).

The code for the player can be found in [/tlog/tlogclient/player/player.go](/tlog/tlogclient/player/player.go).


[tlog]: tlog.md

[log]: /docs/glossary.md#log
[restore]: /docs/glossary.md#restore
[vdisk]: /docs/glossary.md#vdisk
[backend]: /docs/glossary.md#backend
[zeroctl]: /docs/glossary.md#zeroctl

[restorecmd]: /docs/zeroctl/commands/restore.md

