/*Package configV2 is a package for config management for 0-Disk virtual disks.

Config

The config data is stored in 4 sub configs: BaseConfig(general vdisk information), NBDConfig (nbd server information), TlogConfig (tlog server information) and SlaveConfig (slave config information). The information is stored internally in YAML format.

Each sub config has a constructor, ToBytes and a Validate methode.
The constructor will unmarshal a sub config from on a YAML formatted slice of byte, validates the sub config and returns an error if invalid.
ToBytes will marchal the subconfig to a YAML slice of bytes and return it.
Validate will validate the current subconfig. It is however only recommended to be used before setting a subconfig to a source in that source implementation for the configs.

ETCD source

The etcd config source can be used for production allowing for distributed storage of the configs. From the provided endpoint(s) it will connect to the etcd cluster and get the subconfigs according to following keys:
	BaseConfig: 	<VdiskID>:conf:base
	NBDConfig: 	<VdiskID>:conf:nbd
	TlogConfig: 	<VdiskID>:conf:tlog
	SlaveConfig: 	<VdiskID>:conf:slave
Using the etcdv3 Watch API, the etcd source will listen for updates of each subconfig and update the local configs as they come in.
The config stored at the etcd keys are YAML formatted serialized versions of the sub config.

NOTE: For the NBDConfig there should be a valid BaseConfig in the source to validate NBDConfig

File source

The file config source is recommended only for development use and gets it's config data from a provided YAML file location. 1 YAML file can represents the data of 1 vdisk formatted to the configFileFormat type.
It will automatically devide the data in the different subconfigs after reading from the source file.

YAML file example:
	baseConfig:
		blockSize: 4096
		readOnly: false
		size: 10
		type: db
	nbdConfig:
		templateVdiskID: mytemplate
		storageCluster:
			dataStorage:
			- address: 192.168.58.146:2000
				db: 0
			- address: 192.123.123.123:2001
				db: 0
			metadataStorage:
			address: 192.168.58.146:2001
			db: 1
		templateStorageCluster:
			dataStorage:
			- address: 192.168.58.147:2000
				db: 0
	tlogConfig:
		tlogStorageCluster:
			dataStorage:
		current	- address: 192.168.58.149:2000
				db: 4
			metadataStorage:
			address: 192.168.58.146:2001
			db: 8

NOTE: For the NBDConfig there should be a valid BaseConfig in the source to validate NBDConfig

*/
package configV2
