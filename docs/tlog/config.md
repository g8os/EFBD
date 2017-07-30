# Tlog Server Configuration

The [Tlog Server][tlogserver] and its components are normally configured using an [etcd][configetcd] distributed key-value store. For 0-Disk development purposes, one can also use a [file-based][configfile]-based configuration. As you should however use the [etcd-based][configetcd] configuration, especially in production, we'll use it in all examples that follow.

The [TLog Server][tlogserver] requires the [Tlog Configuration](#tlog-config) and [Slave Configuration](#slave-config). See [the 0-Disk config overview page][configDoc] for more elaborate information of each of these sub configurations and more.

For more information about how to configure the [Tlog Server][tlogserver] using [etcd][etcd], check out [the etcd config docs][configetcd].

## Tlog Config

The TLog config is the only required subconfig used by the [TLog Server][tlogserver]. It contains all the information required to store the ([meta][metadata])[data][data] of all logged transactions for the [vdisks][vdisk] which are subscribed to this [TLog Server][tlogserver].

> This config is **REQUIRED**. A [vdisk][vdisk] which does not define this configuration correctly, cannot make use of the [TLog Server][tlogserver], and will result in undefined behavior! However from a [vdisk][vdisk]-usage perspective this config is **OPTIONAL**, and giving it will simply enable the tlog feature for those [vdisk][vdisk] types that support it.

This config is stored in etcd under the `<vdiskid>:zerodisk:conf:tlog` key format. For a [vdisk][vdisk] named `foo`, this key would thus become `foo:zerodisk:conf:tlog`. An example of its YAML-formatted value could look as follows:

```yaml
storageCluster: # required (to be deprecated as soon as the 0-stor is available)
  dataStorage: # m+k servers are required
    - address: 192.168.1.1:1000 # required dialstring
      db: 14 # optional, 0 by default
  metadataStorage:
    address: 192.168.1.1:1001
    db: 18
slaveSync: true # optional, false by default
                # when enabled, all incoming transactions will be
                # written to the Slave Storage Cluster as well,
                # which serves as a on-the-go backup cluster,
                # should the primary storage cluster (partially) fail
```

> NOTE that the TLog Storage Cluster is planned to become deprecated soon, and will be replaced with 0-Stor configuration as soon as it becomes available.

See the [tlog config docs][tlogconf] on the [0-Disk config overview page][configDoc] for more information.

See the [TlogConfig Godoc][tlogconfigGodoc] for more information about the internal workings and how it is implemented in Go.

## Slave Config

The Slave config is the last subconfig used by the [TLog Server][tlogserver] and is optional. Only used by [boot][boot] and [db][db] [vdisks][vdisk] which have a valid [TLog Configuration](#tlog-config), which has the `slaveSync` set to `true`.

> This config is **OPTIONAL**. However if the [TLog Configuration](#tlog-config) of this vdisk is specified and used, this config becomes **REQUIRED**. If in this case a [vdisk][vdisk] does not define this configuration (correctly), errors will be triggered and the result is be undefined.

This config is stored in etcd under the `<vdiskid>:zerodisk:conf:slave` key format. For a [vdisk][vdisk] named `foo`, this key would thus become `foo:zerodisk:conf:slave`. An example of its YAML-formatted value could look as follows:

```yaml
storageCluster: # required
  dataStorage:  # at least 1 server required
    - address: 192.168.2.149:1000 # required dialstring
      db: 14 # optional, 0 by default
  metadataStorage: # not used for nondeduped vdisks, required for all others
    address: 192.168.2.146:1001
    db: 18
```

See the [slave config docs][slaveconf] on the [0-Disk config overview page][configDoc] for more information.

See the [SlaveConfig Godoc][slaveconfigGodoc] for more information about the internal workings and how it is implemented in Go.

[tlogserver]: server.md

[configDoc]: /docs/config.md
[configetcd]: /docs/config.md#etcd
[configfile]: /docs/config.md#file

[tlogconf]: /docs/config.md#tlog-config
[slaveconf]: /docs/config.md#slave-config

[etcd]: https://github.com/coreos/etcd

[metadata]: glossary.md#metadata
[data]: glossary.md#data
[vdisk]: /docs/glossary.md#vdisk
[boot]: glossary.md#boot
[db]: glossary.md#db

[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[tlogconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#TlogConfig
[slaveconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#SlaveConfig