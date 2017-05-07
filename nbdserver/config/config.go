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
	_, err = valid.ValidateStruct(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create nbdserver config: %s", err.Error())
	}

	return cfg, nil
}

// Config for the nbdserver backends
type Config struct {
	StorageClusters map[string]StorageClusterConfig `yaml:"storageclusters" valid:"required"`
	Vdisks          map[string]VdiskConfig          `yaml:"vdisks" valid:"required"`
}

// StorageClusterConfig defines the config for a storagecluster
type StorageClusterConfig struct {
	DataStorage     []string `yaml:"dataStorage" valid:"dialstring,length(1),required"`
	MetaDataStorage string   `yaml:"metadataStorage" valid:"dialstring,required"`
}

// VdiskConfig defines the config for a vdisk
type VdiskConfig struct {
	Blocksize          uint64    `yaml:"blocksize" valid:"required"`
	ReadOnly           bool      `yaml:"readOnly" valid:"optional"`
	Size               uint64    `yaml:"size" valid:"required"`
	Storagecluster     string    `yaml:"storagecluster" valid:"required"`
	RootDataStorage    string    `yaml:"rootDataStorage" valid:"dialstring,optional"`
	TlogStoragecluster string    `yaml:"tlogStoragecluster" valid:"optional"`
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
