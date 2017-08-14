package config

var validNBDStorageConfigs = []NBDStorageConfig{
	// complete example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
		},
		TemplateStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "123.123.123.123:300",
					Database: 4,
				},
				StorageServerConfig{
					Address:  "123.123.123.124:300",
					Database: 2,
				},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "foo:16320",
					Database: 4,
				},
				StorageServerConfig{
					Address:  "foo:16321",
					Database: 5,
				},
			},
		},
	},
	// minimal example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	},
}

var validNBDStorageConfigsDeduped = []NBDStorageConfig{
	// complete example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
			MetadataStorage: &StorageServerConfig{
				Address:  "localhost:16378",
				Database: 4,
			},
		},
		TemplateStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "123.123.123.123:300",
					Database: 3,
				},
				StorageServerConfig{
					Address:  "123.123.123.124:300",
					Database: 6,
				},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "slave:16379",
					Database: 5,
				},
				StorageServerConfig{
					Address:  "slave:16380",
					Database: 3,
				},
			},
			MetadataStorage: &StorageServerConfig{
				Address:  "slave:16378",
				Database: 1,
			},
		},
	},
	// minimal example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
			MetadataStorage: &StorageServerConfig{
				Address: "localhost:16379",
			},
		},
	},
}

var invalidNBDStorageConfigs = []NBDStorageConfig{
	// missing primary storage server
	NBDStorageConfig{},
	// invalid primary storage server (no data storage given)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
		},
	},
	// template storage given, but is invalid
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16381"},
			},
		},
		TemplateStorageCluster: new(StorageClusterConfig),
	},
	// template storage given, but is invalid
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16381"},
			},
		},
		SlaveStorageCluster: new(StorageClusterConfig),
	},
}

var invalidNBDStorageConfigsDeduped = append(invalidNBDStorageConfigs,
	// missing metadata server (primary)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16379"},
			},
		},
	},
	// missing metadata server (slave)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16379"},
			},
			MetadataStorage: &StorageServerConfig{
				Address: "localhost:16379",
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16379"},
			},
		},
	})

var validTlogStorageConfigs = []TlogStorageConfig{
	// complete example
	TlogStorageConfig{
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
		},
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
	},
	// minimal example
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
	},
}

var validTlogStorageConfigsDeduped = []TlogStorageConfig{
	// complete example
	TlogStorageConfig{
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
			MetadataStorage: &StorageServerConfig{
				Address:  "localhost:42",
				Database: 5,
			},
		},
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
	},
	// minimal example
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
	},
}

var invalidTlogStorageConfigs = []TlogStorageConfig{
	// missing Storage Cluster
	TlogStorageConfig{},
	// invalid ZeroStor Cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
	},
	// invalid ZeroStor Storage Cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
		},
	},
}

var invalidTlogStorageConfigsDeduped = append(invalidTlogStorageConfigs,
	// missing metadata storage server in given slave cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []Server{
				Server{Address: "1.1.1.1:11"},
				Server{Address: "2.2.2.2:22"},
			},
			MetadataServers: []Server{
				Server{Address: "3.3.3.3:33"},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	})
