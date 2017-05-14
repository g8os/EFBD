package config

import (
	"fmt"
	"io/ioutil"

	valid "github.com/asaskevich/govalidator"
	"github.com/go-yaml/yaml"
)

// ReadConfig reads the config used to configure the nbdserver
func ReadConfig(path string) (*Config, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't read nbdserver config: %s", err.Error())
	}

	return FromBytes(bytes)
}

// FromBytes creates a config based on given YAML 1.2 content
func FromBytes(bytes []byte) (*Config, error) {
	cfg := new(Config)

	// unmarshal the yaml content into our config,
	// which will give us basic validation guarantees
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create nbdserver config: %s", err.Error())
	}

	// now apply an extra validation layer,
	// to ensure that the values with generic types,
	// are also valid
	err = cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("couldn't create nbdserver config: %s", err.Error())
	}

	return cfg, nil
}

// Config for the nbdserver backends
type Config struct {
	StorageClusters map[string]StorageClusterConfig `yaml:"storageClusters" valid:"required"`
	Vdisks          map[string]VdiskConfig          `yaml:"vdisks" valid:"required"`
}

// Validate the Config to ensure that all required properties are present,
// and that all given properties are valid
func (cfg *Config) Validate() error {
	if _, err := valid.ValidateStruct(cfg); err != nil {
		return err
	}

	// ensure all referenced storage clusters exist, O(n^2)
	for vdiskID, vdisk := range cfg.Vdisks {
		var storageClusterExists, rootStorageClusterExists bool

		// cache storage cluster references
		storageCluster := vdisk.Storagecluster
		rootStorageCluster := vdisk.RootStorageCluster

		if rootStorageCluster == "" {
			// non-reference always exists
			rootStorageClusterExists = true
		}

		// go through all storageClusters
		for clusterID := range cfg.StorageClusters {
			if !storageClusterExists && storageCluster == clusterID {
				storageClusterExists = true
				if rootStorageClusterExists {
					break // both references exists, early break
				}
			} else if !rootStorageClusterExists && rootStorageCluster == clusterID {
				rootStorageClusterExists = true
				if storageClusterExists {
					break // both references exists, early break
				}
			}
		}

		// ensure referenced (root) storage cluster exists
		if !storageClusterExists {
			return fmt.Errorf(
				"vdisk %q references unexisting storage cluster %q",
				vdiskID, storageCluster)
		}
		if !rootStorageClusterExists {
			return fmt.Errorf(
				"vdisk %q references unexisting root storage cluster %q",
				vdiskID, rootStorageCluster)
		}
	}

	// valid
	return nil
}

// StorageClusterConfig defines the config for a storageCluster
type StorageClusterConfig struct {
	DataStorage     []string `yaml:"dataStorage" valid:"dialstring,required"`
	MetaDataStorage string   `yaml:"metadataStorage" valid:"dialstring,required"`
}

// VdiskConfig defines the config for a vdisk
type VdiskConfig struct {
	Blocksize          uint64    `yaml:"blocksize" valid:"required"`
	ReadOnly           bool      `yaml:"readOnly" valid:"optional"`
	Size               uint64    `yaml:"size" valid:"required"`
	Storagecluster     string    `yaml:"storageCluster" valid:"required"`
	RootStorageCluster string    `yaml:"rootStorageCluster" valid:"optional"`
	TlogStoragecluster string    `yaml:"tlogStorageCluster" valid:"optional"`
	Type               VdiskType `yaml:"type" valid:"required"`
}

// VdiskType represents the type of a vdisk
type VdiskType string

// MarshalYAML implements yaml.Marshaler.MarshalYAML
func (t *VdiskType) MarshalYAML() (interface{}, error) {
	switch vt := *t; vt {
	case VdiskTypeBoot, VdiskTypeDB, VdiskTypeCache:
		return string(vt), nil
	default:
		return nil, fmt.Errorf("%q is not a valid VdiskType", t)
	}
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (t *VdiskType) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawType string
	err = unmarshal(&rawType)
	if err != nil {
		return fmt.Errorf("%q is not a valid VdiskType", t)
	}

	vtype := VdiskType(rawType)
	switch vtype {
	case VdiskTypeBoot, VdiskTypeDB, VdiskTypeCache:
		*t = vtype
	default:
		err = fmt.Errorf("%q is not a valid VdiskType", t)
	}

	return
}

// valid vdisk types
const (
	VdiskTypeNil   = VdiskType("")
	VdiskTypeBoot  = VdiskType("boot")
	VdiskTypeDB    = VdiskType("db")
	VdiskTypeCache = VdiskType("cache")
)

func init() {
	valid.SetFieldsRequiredByDefault(true)
}
