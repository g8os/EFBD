package config

import (
	"fmt"

	valid "github.com/asaskevich/govalidator"
	yaml "gopkg.in/yaml.v2"
)

// format.go
// defines all config format structs,
// that is to say the structs which get deserialized
// directly from YAML data

// FormatValidator defines the validator interface of each format struct.
// All Content is serialized in the YAML 1.2 format.
type FormatValidator interface {
	// Validate the parsed format,
	// such that deserialized content is only used if valid.
	Validate() error
}

// NewNBDVdisksConfig creates a new NBDVdisksConfig from a given YAML slice.
func NewNBDVdisksConfig(data []byte) (*NBDVdisksConfig, error) {
	vdiskscfg := new(NBDVdisksConfig)
	err := yaml.UnmarshalStrict(data, &vdiskscfg)
	if err != nil {
		return nil, err
	}

	err = vdiskscfg.Validate()
	if err != nil {
		return nil, err
	}

	return vdiskscfg, nil
}

// NBDVdisksConfig contains a list of all Vdisks
// exposed by a given NBD Server.
type NBDVdisksConfig struct {
	Vdisks []string `yaml:"vdisks" valid:"required"`
}

// NewVdiskStaticConfig creates a new VdiskStaticConfig from a given YAML slice.
func NewVdiskStaticConfig(data []byte) (*VdiskStaticConfig, error) {
	staticfg := new(VdiskStaticConfig)
	err := yaml.UnmarshalStrict(data, &staticfg)
	if err != nil {
		return nil, err
	}

	err = staticfg.Validate()
	if err != nil {
		return nil, err
	}

	return staticfg, nil
}

// Validate implements FormatValidator.Validate.
func (cfg *NBDVdisksConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid NBDVdisksConfig: %v", err)
	}

	return nil
}

// VdiskStaticConfig represents the static info of a vdisk.
type VdiskStaticConfig struct {
	BlockSize uint64    `yaml:"blockSize" valid:"required"`
	ReadOnly  bool      `yaml:"readOnly" valid:"optional"`
	Size      uint64    `yaml:"size" valid:"required"`
	Type      VdiskType `yaml:"type" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskStaticConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	// check valid tags
	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid static config: %v", err)
	}

	// validate properties in more detail

	if x := cfg.BlockSize; x == 0 || (x&(x-1)) != 0 {
		return fmt.Errorf(
			"blockSize '%d' is not a power of 2, while that is required", cfg.BlockSize)
	}
	if (cfg.Size * gibibyteAsBytes) < cfg.BlockSize {
		return fmt.Errorf(
			"%d is an invalid size, has to be able to contain at least 1 block", cfg.Size)
	}
	err = cfg.Type.Validate()
	if err != nil {
		return fmt.Errorf("VdiskStaticConfig has invalid type: %s", err.Error())
	}

	return nil
}

// NewVdiskNBDConfig creates a new VdiskNBDConfig from a given YAML slice.
func NewVdiskNBDConfig(data []byte) (*VdiskNBDConfig, error) {
	nbdcfg := new(VdiskNBDConfig)

	err := yaml.UnmarshalStrict(data, nbdcfg)
	if err != nil {
		return nil, err
	}

	err = nbdcfg.Validate()
	if err != nil {
		return nil, err
	}

	return nbdcfg, nil
}

// VdiskNBDConfig represents the nbdserver-related information for a vdisk.
type VdiskNBDConfig struct {
	StorageClusterID         string `yaml:"storageClusterID" valid:"required"`
	TemplateStorageClusterID string `yaml:"templateStorageClusterID" valid:"optional"`
	TemplateVdiskID          string `yaml:"templateVdiskID" valid:"optional"`
	TlogServerClusterID      string `yaml:"tlogServerClusterID" valid:"optional"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskNBDConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid VdiskNBDConfig: %v", err)
	}

	if cfg.TemplateVdiskID != "" && cfg.TemplateStorageClusterID == "" {
		return fmt.Errorf(
			"invalid VdiskNBDConfig: 'templateVdiskID' field is defined (%s)"+
				"while 'templateStorageClusterID' field is undefined",
			cfg.TemplateVdiskID)
	}

	return nil
}

// NewVdiskTlogConfig creates a new VdiskTlogConfig from a given YAML slice.
func NewVdiskTlogConfig(data []byte) (*VdiskTlogConfig, error) {
	tlogcfg := new(VdiskTlogConfig)

	err := yaml.UnmarshalStrict(data, tlogcfg)
	if err != nil {
		return nil, err
	}

	err = tlogcfg.Validate()
	if err != nil {
		return nil, err
	}

	return tlogcfg, nil
}

// VdiskTlogConfig represents the tlogserver-related information for a vdisk.
type VdiskTlogConfig struct {
	StorageClusterID      string `yaml:"storageClusterID" valid:"required"`
	SlaveStorageClusterID string `yaml:"slaveStorageClusterID" valid:"optional"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskTlogConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid VdiskTlogConfig: %v", err)
	}

	return nil
}

// NewStorageClusterConfig creates a new StorageClusterConfig from a given YAML slice.
func NewStorageClusterConfig(data []byte) (*StorageClusterConfig, error) {
	clustercfg := new(StorageClusterConfig)

	err := yaml.UnmarshalStrict(data, clustercfg)
	if err != nil {
		return nil, err
	}

	err = clustercfg.Validate()
	if err != nil {
		return nil, err
	}

	return clustercfg, nil
}

// StorageClusterConfig defines the config for a storageCluster.
// A storage cluster is composed out of multiple data storage servers,
// and a single (optional) metadata storage.
type StorageClusterConfig struct {
	DataStorage     []StorageServerConfig `yaml:"dataStorage" valid:"required"`
	MetadataStorage *StorageServerConfig  `yaml:"metadataStorage" valid:"optional"`
}

// Validate implements FormatValidator.Validate.
func (cfg *StorageClusterConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid StorageClusterConfig: %v", err)
	}

	return nil
}

// Clone implements Cloner.Clone
func (cfg *StorageClusterConfig) Clone() StorageClusterConfig {
	var clone StorageClusterConfig
	if cfg == nil {
		return clone
	}

	clone.DataStorage = make([]StorageServerConfig, len(clone.DataStorage))
	copy(clone.DataStorage, cfg.DataStorage)

	if cfg.MetadataStorage != nil {
		storage := *cfg.MetadataStorage
		clone.MetadataStorage = &storage
	}

	return clone
}

// NewTlogClusterConfig creates a new TlogClusterConfig from a given YAML slice.
func NewTlogClusterConfig(data []byte) (*TlogClusterConfig, error) {
	clustercfg := new(TlogClusterConfig)

	err := yaml.UnmarshalStrict(data, clustercfg)
	if err != nil {
		return nil, err
	}

	err = clustercfg.Validate()
	if err != nil {
		return nil, err
	}

	return clustercfg, nil
}

// TlogClusterConfig defines the config for a Tlog Server Custer.
// A Tlog Server cluster is composed out of one or more Tlog servers.
type TlogClusterConfig struct {
	Servers []TlogServerConfig `yaml:"servers" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *TlogClusterConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return fmt.Errorf("invalid TlogClusterConfig: %v", err)
	}

	return nil
}

// TlogServerConfig defines the config for a Tlog server
type TlogServerConfig struct {
	Address string `yaml:"address" valid:"dialstring,required"`
}

// StorageServerConfig defines the config for a storage server
type StorageServerConfig struct {
	Address string `yaml:"address" valid:"dialstring,required"`
	// Database '0' is assumed, in case no value is given.
	Database int `yaml:"db" valid:"optional"`
}

const (
	// gibibyteAsBytes is a constant used to convert between GiB and bytes
	gibibyteAsBytes = 1024 * 1024 * 1024
)

func init() {
	valid.SetFieldsRequiredByDefault(true)
}
