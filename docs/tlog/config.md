# Tlog Server Configuration

The Tlog server is configured using an etcd distributed key-value store.

The 0-Disk Config package contains 4 subconfigs:
  * BaseConfig
  * NBDConfig
  * TlogConfig
  * SlaveConfig

For the Tlog Server TlogConfig will be used to store the Tlog server configuration

The Tlog Server will use the methodes provided by the config package to communicate with the etcd storage cluster. It will use the Read\<Sub\>ConfigETCD methodes for getting the data once. If it needs the data to be updated the Tlog server will use the channel returned by Watch\<Sub\>ConfigETCD to receive updates on the subconfig and store it.

Within the Tlog server, it uses the configs in following places:
  * server.vdisk: to read TlogSlaveSync and check from TlogConfig
  * slavesync: to read TlogStorageCluster

More details about the subconfigs, etcd API can be found on the [config documentation page][configDoc]

[storage]: /docs/glossary.md#storage