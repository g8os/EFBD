package config

import (
	"errors"
	"fmt"

	valid "github.com/asaskevich/govalidator"
	"github.com/go-yaml/yaml"
)

// BaseConfig represents the basic vdisk info
type BaseConfig struct {
	BlockSize uint64    `yaml:"blockSize" valid:"required"`
	ReadOnly  bool      `yaml:"readOnly" valid:"optional"`
	Size      uint64    `yaml:"size" valid:"required"`
	Type      VdiskType `yaml:"type" valid:"required"`
}

// NewBaseConfig creates a new Baseconfig from byte slice in YAML 1.2 format
func NewBaseConfig(data []byte) (*BaseConfig, error) {
	base := new(BaseConfig)
	err := yaml.Unmarshal(data, &base)
	if err != nil {
		return nil, err
	}

	err = base.Validate()
	if err != nil {
		return nil, err
	}

	return base, nil
}

// ToBytes converts baseConfig in byte slice in YAML 1.2 format
func (base *BaseConfig) ToBytes() ([]byte, error) {
	res, err := yaml.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("failed to turn base config into bytes: %v", err)
	}

	return res, nil
}

// Validate Validates baseConfig
// Should only be used for ConfigSource implementation
func (base BaseConfig) Validate() error {
	// check valid tags
	_, err := valid.ValidateStruct(base)
	if err != nil {
		return fmt.Errorf("invalid base config: %v", err)
	}

	// check base properties
	if base.BlockSize == 0 || base.BlockSize%2 != 0 {
		return fmt.Errorf("%d is an invalid blockSize", base.BlockSize)
	}
	if base.Size == 0 {
		return fmt.Errorf("%d is an invalid size", base.Size)
	}
	err = base.Type.Validate()
	if err != nil {
		return fmt.Errorf("baseconfig has invalid type: %s", err.Error())

	}

	return nil
}

// NBDConfig represents an nbd storage configuration
type NBDConfig struct {
	StorageCluster         StorageClusterConfig  `yaml:"storageCluster" valid:"required"`
	TemplateStorageCluster *StorageClusterConfig `yaml:"templateStorageCluster" valid:"optional"`
	TemplateVdiskID        string                `yaml:"templateVdiskID" valid:"optional"`
	TlogRPC                string                `yaml:"tlogrpc" valid:"optional"`
}

// NewNBDConfig creates a new NBDConfig from byte slice in YAML 1.2 format
func NewNBDConfig(data []byte, vdiskType VdiskType) (*NBDConfig, error) {
	if vdiskType.Validate() != nil {
		return nil, errors.New("Invalid vdisk type was given to the NBD constructor")
	}
	nbd := new(NBDConfig)
	err := yaml.Unmarshal(data, nbd)
	if err != nil {
		return nil, err
	}
	err = nbd.Validate(vdiskType)
	if err != nil {
		return nil, err
	}

	return nbd, nil
}

// ToBytes converts NBDConfig in byte slice in YAML 1.2 format
func (nbd *NBDConfig) ToBytes() ([]byte, error) {
	res, err := yaml.Marshal(nbd)
	if err != nil {
		return nil, fmt.Errorf("failed to turn nbd config into bytes: %v", err)
	}

	return res, nil
}

// Validate Validates NBDConfig
// needs a vdisk type
// Should only be used for ConfigSource implementation
func (nbd *NBDConfig) Validate(vdiskType VdiskType) error {
	// nbd is optional so if nil return
	if nbd == nil {
		return nil
	}

	_, err := valid.ValidateStruct(nbd)
	if err != nil {
		return fmt.Errorf("invalid NBD config: %v", err)
	}

	if len(nbd.StorageCluster.DataStorage) <= 0 {
		return fmt.Errorf("nbd datastorage was empty")
	}

	// validate if metadata storage is defined when required
	metadataUndefined := nbd.StorageCluster.MetadataStorage == nil
	if metadataUndefined && vdiskType.StorageType() == StorageDeduped {
		return fmt.Errorf("metadata storage not found while required")
	}

	return nil
}

// TlogConfig represents a tlog storage configuration
type TlogConfig struct {
	TlogStorageCluster StorageClusterConfig `yaml:"tlogStorageCluster" valid:"optional"`
	SlaveSync          bool                 `yaml:"tlogSlaveSync" valid:"optional"`
}

// NewTlogConfig creates a new Tlogconfig from byte slice in YAML 1.2 format
func NewTlogConfig(data []byte) (*TlogConfig, error) {
	tlog := new(TlogConfig)
	err := yaml.Unmarshal(data, tlog)
	if err != nil {
		return nil, err
	}
	err = tlog.Validate()
	if err != nil {
		return nil, err
	}

	return tlog, nil
}

// ToBytes converts TlogConfig in byte slice in YAML 1.2 format
func (tlog *TlogConfig) ToBytes() ([]byte, error) {
	res, err := yaml.Marshal(tlog)
	if err != nil {
		return nil, fmt.Errorf("failed to turn tlog config into bytes: %v", err)
	}

	return res, nil
}

// Validate Validates TlogConfig
// Should only be used for ConfigSource implementation
func (tlog *TlogConfig) Validate() error {
	// tlog is optional so if nil return
	if tlog == nil {
		return nil
	}

	_, err := valid.ValidateStruct(tlog)
	if err != nil {
		return fmt.Errorf("invalid tlog config: %v", err)
	}

	if len(tlog.TlogStorageCluster.DataStorage) <= 0 {
		return fmt.Errorf("no tlog datastorage was found")
	}

	return nil
}

// SlaveConfig represents a backup storage configuration
type SlaveConfig struct {
	SlaveStorageCluster StorageClusterConfig `yaml:"slaveStorageCluster" valid:"optional"`
}

// NewSlaveConfig creates a new Slaveconfig from byte slice in YAML 1.2 format
func NewSlaveConfig(data []byte) (*SlaveConfig, error) {
	slave := new(SlaveConfig)
	err := yaml.Unmarshal(data, slave)
	if err != nil {
		return nil, err
	}
	err = slave.Validate()
	if err != nil {
		return nil, err
	}

	return slave, nil
}

// ToBytes converts SlaveConfig in byte slice in YAML 1.2 format
func (slave *SlaveConfig) ToBytes() ([]byte, error) {
	res, err := yaml.Marshal(slave)
	if err != nil {
		return nil, fmt.Errorf("failed to turn slave config into bytes: %v", err)
	}

	return res, nil
}

// Validate Validates SlaveConfig
// Should only be used for ConfigSource implementation
func (slave *SlaveConfig) Validate() error {
	// slave is optional so if nil return
	if slave == nil {
		return nil
	}

	_, err := valid.ValidateStruct(slave)
	if err != nil {
		return fmt.Errorf("invalid slave config: %v", err)
	}

	return nil
}

// VdisksConfig represents the vdisks config. It is used by the NBDServer,
// as the NBD Protocol requires a list of available export names (vdisk IDs),
// for some situations.
type VdisksConfig struct {
	List []string `yaml:"vdisks" valid:"required"`
}

// NewVdisksConfig creates a new VdisksConfig
// from byte slice in YAML 1.2 format
func NewVdisksConfig(data []byte) (*VdisksConfig, error) {
	vdisks := new(VdisksConfig)
	err := yaml.Unmarshal(data, vdisks)
	if err != nil {
		return nil, err
	}
	err = vdisks.Validate()
	if err != nil {
		return nil, err
	}

	return vdisks, nil
}

// ToBytes converts VdisksConfig in byte slice in YAML 1.2 format
func (vdisks *VdisksConfig) ToBytes() ([]byte, error) {
	res, err := yaml.Marshal(vdisks)
	if err != nil {
		return nil, fmt.Errorf("failed to turn vdisks config into bytes: %v", err)
	}

	return res, nil
}

// Validate Validates VdisksConfig
// Should only be used for ConfigSource implementation
func (vdisks *VdisksConfig) Validate() error {
	// slave is optional so if nil return
	if vdisks == nil {
		return nil
	}

	_, err := valid.ValidateStruct(vdisks)
	if err != nil {
		return fmt.Errorf("invalid vdisks config: %v", err)
	}

	return nil
}

// VdiskType represents the type of a vdisk,
// and each valid bit defines a property of the vdisk,
// and its the different collections of valid bits that defines
// each valid and unique type.
type VdiskType uint8

// TlogSupport returns whether or not the data of this vdisk
// has to send to the tlog server, to log its transactions.
func (vdiskType VdiskType) TlogSupport() bool {
	return vdiskType&propTlogSupport != 0
}

// TemplateSupport returns whether or not
// this vdisk supports a template server,
// to get the data in case the data isn't available on
// the normal (local) storage cluster.
func (vdiskType VdiskType) TemplateSupport() bool {
	return vdiskType&propTemplateSupport != 0
}

// Vdisk Properties
const (
	// All content is deduped,
	// meaning that only unique content (blocks) are stored.
	propDeduped VdiskType = 1 << iota
	// Content is stored in external storage servers.
	propPersistent
	// Content is only available during the session of creation,
	// and is released from RAM when shutting down the vdisk.
	propTemporary
	// Each write data transaction (write/delete/merge),
	// is also logged to a tlogserver (if one is given),
	// allowing for rollbacks and replays of the data.
	propTlogSupport
	// Allows data to be read from a root storage cluster,
	// in case it isn't available in the (local) storage cluster yet,
	// storing it as well (async) in the (local) storage cluster when read.
	propTemplateSupport
)

// vdisktype strings
const (
	vdiskTypeNilStr   = ""
	vdiskTypeBootStr  = "boot"
	vdiskTypeDBStr    = "db"
	vdiskTypeCacheStr = "cache"
	vdiskTypeTmpStr   = "tmp"
)

// valid vdisk types
// based on /docs/README.md#zero-os-0-disk
const (
	VdiskTypeBoot  = propDeduped | propPersistent | propTlogSupport | propTemplateSupport
	VdiskTypeDB    = propPersistent | propTlogSupport | propTemplateSupport
	VdiskTypeCache = propPersistent
	VdiskTypeTmp   = propTemporary
)

// StorageType returns the type of storage this vdisk uses
func (vdiskType VdiskType) StorageType() StorageType {
	if vdiskType&propDeduped != 0 {
		return StorageDeduped
	}

	// TODO: Handle propPersistent flag
	// ignore the propPersistent flag for now,
	// and treat non-persistent and persistent memory,
	// both as persistent nondeduped storage.
	// see open issue for more information:
	// https://github.com/zero-os/0-Disk/issues/222

	return StorageNonDeduped
}

// Validate this vdisk type
func (vdiskType VdiskType) Validate() error {
	switch vdiskType {
	case VdiskTypeBoot, VdiskTypeDB, VdiskTypeCache, VdiskTypeTmp:
		return nil
	default:
		return fmt.Errorf("%s is an invalid VdiskType", vdiskType)
	}
}

// String returns the storage type as a string value
func (vdiskType VdiskType) String() string {
	switch vdiskType {
	case VdiskTypeBoot:
		return vdiskTypeBootStr
	case VdiskTypeDB:
		return vdiskTypeDBStr
	case VdiskTypeCache:
		return vdiskTypeCacheStr
	case VdiskTypeTmp:
		return vdiskTypeTmpStr
	default:
		return vdiskTypeNilStr
	}
}

// SetString allows you to set this VdiskType using
// the correct string representation
func (vdiskType *VdiskType) SetString(s string) error {
	switch s {
	case vdiskTypeBootStr:
		*vdiskType = VdiskTypeBoot
	case vdiskTypeDBStr:
		*vdiskType = VdiskTypeDB
	case vdiskTypeCacheStr:
		*vdiskType = VdiskTypeCache
	case vdiskTypeTmpStr:
		*vdiskType = VdiskTypeTmp
	default:
		return fmt.Errorf("%q is not a valid VdiskType", s)
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler.MarshalYAML
func (vdiskType VdiskType) MarshalYAML() (interface{}, error) {
	if s := vdiskType.String(); s != vdiskTypeNilStr {
		return s, nil
	}

	return nil, fmt.Errorf("%v is not a valid VdiskType", vdiskType)
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (vdiskType *VdiskType) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawType string
	err = unmarshal(&rawType)
	if err != nil {
		return fmt.Errorf("%q is not a valid VdiskType: %s", vdiskType, err)
	}

	err = vdiskType.SetString(rawType)
	return err
}

// StorageServerConfig defines the config for a storage server
type StorageServerConfig struct {
	Address string `yaml:"address" valid:"dialstring,required"`
	// Database '0' is assumed, in case no value is given.
	Database int `yaml:"db" valid:"optional"`
}

// StorageClusterConfig defines the config for a storageCluster.
// A storage cluster is composed out of multiple data storage servers,
// and a single (optional) metadata storage.
//
// NOTE: the meta data storage is planned to be removed,
// as having the metadata stored in a single servers,
// makes it a critical point of failure, for some of its use cases.
type StorageClusterConfig struct {
	DataStorage     []StorageServerConfig `yaml:"dataStorage" valid:"required"`
	MetadataStorage *StorageServerConfig  `yaml:"metadataStorage" valid:"optional"`
}

// StorageType represents the type of storage of a vdisk
type StorageType uint8

// Different types of storage
const (
	StorageNil     StorageType = 0
	StorageDeduped StorageType = 1 << iota
	StorageNonDeduped
	// StorageSemiDeduped is not used for now
	StorageSemiDeduped
)

// UInt8 returns the storage type as an uint8 value
func (st StorageType) UInt8() uint8 {
	return uint8(st)
}

// String returns the name of the storage type
func (st StorageType) String() string {
	switch st {
	case StorageDeduped:
		return "deduped"
	case StorageNonDeduped:
		return "nondeduped"
	case StorageSemiDeduped:
		return "semideduped"
	default:
		return "unknown"
	}
}
