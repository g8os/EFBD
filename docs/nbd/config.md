# NBD Server Configuration

The NBD server and its backend are configured using an etcd distributed key-value store.

The 0-Disk Config package contains 4 subconfigs:
  * BaseConfig
  * NBDConfig
  * TlogConfig
  * SlaveConfig

For the NBD Server the BaseConfig, NBDConfig and SlaveConfig will be used to store the NBD server configuration.

The NBD Server will use the methodes provided by the config package to communicate with the etcd storage cluster. It will use the Read\<Sub\>ConfigETCD methodes for getting the data once. If it needs the data to be updated the NBD server will use the channel returned by Watch\<Sub\>ConfigETCD to receive updates on the subconfig and store it.

Within the NBD server, it uses the configs in following places:
  * ardb: use the BaseConfig and NBDConfig for the BackendFactory
  * nbdserver.tlog : when switching to the ardb slave the SlaveConfig will be used

More details about the subconfigs, etcd API can be found on the [config documentation page][configDoc]

[configDoc]: /docs/config.md