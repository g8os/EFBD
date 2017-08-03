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
}

var invalidNBDStorageConfigsDeduped = append(invalidNBDStorageConfigs,
	// missing metadata server
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16379"},
			},
		},
	})

var validTlogStorageConfigs = []TlogStorageConfig{
	// complete example
	TlogStorageConfig{
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
	},
	// minimal example
	TlogStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	},
}

var validTlogStorageConfigsDeduped = []TlogStorageConfig{
	// complete example
	TlogStorageConfig{
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
	},
	// minimal example
	TlogStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	},
}

var invalidTlogStorageConfigs = []TlogStorageConfig{
	// missing Storage Custer
	TlogStorageConfig{},
	// invalid Storage Cluster
	TlogStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Database: 1,
				},
			},
		},
	},
	// invalid Slave Storage Cluster
	TlogStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
		SlaveStorageCluster: new(StorageClusterConfig),
	},
}

var invalidTlogStorageConfigsDeduped = append(invalidTlogStorageConfigs,
	// missing metadata storage server in given slave cluster
	TlogStorageConfig{
		StorageCluster: StorageClusterConfig{
			DataStorage: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
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
