package configV2

import (
	"fmt"
	"strconv"

	valid "github.com/asaskevich/govalidator"
	"github.com/go-yaml/yaml"
	"github.com/siddontang/go/log"
)

// Config for the zerodisk backends
type Config struct {
	Vdisks map[string]VdiskConfig `yaml:"vdisks" valid:"required"`
}

func (cfg *Config) validate() error {
	for id, vdisk := range cfg.Vdisks {

		// check base config
		err := vdisk.Base.validate()
		if err != nil {
			return fmt.Errorf("could not validate vdisk %s base config: %s", id, err)
		}

		// check ndb config
		err = vdisk.NDB.validate(id, vdisk.Base.Type)
		if err != nil {
			return fmt.Errorf("could not validate vdisk %s ndb config: %s", id, err)
		}

		// check tlog
		err = vdisk.Tlog.validate()
		if err != nil {
			return fmt.Errorf("could not validate vdisk %s tlog config: %s", id, err)
		}

		// check slave
		err = vdisk.Slave.validate()
		if err != nil {
			return fmt.Errorf("could not validate vdisk %s slave config: %s", id, err)
		}
	}

	return nil
}

// VdiskConfig represents a vdisk config
type VdiskConfig struct {
	Base  BaseConfig  `yaml:"baseConfig" valid:"required"`
	NDB   NDBConfig   `yaml:"ndbConfig" valid:"required"`
	Tlog  TlogConfig  `yaml:"tlogConfig" valid:"required"`
	Slave SlaveConfig `yaml:"slaveConfig" valid:"optional"`
}

// BaseConfig represents the basic vdisk info
type BaseConfig struct {
	BlockSize uint64    `yaml:"blockSize" valid:"optional"`
	ReadOnly  bool      `yaml:"readOnly" valid:"optional"`
	Size      uint64    `yaml:"size" valid:"optional"`
	Type      VdiskType `yaml:"type" valid:"optional"`
}

// NewBaseConfig creates a new Baseconfig from byte slice in YAML 1.2 format
func NewBaseConfig(data []byte) (*BaseConfig, error) {
	base := new(BaseConfig)
	err := base.deserialize(data)
	if err != nil {
		return nil, err
	}
	base.validate()
	if err != nil {
		return nil, err
	}
	return base, nil
}

// serialize converts baseConfig in byte slice in YAML 1.2 format
func (base *BaseConfig) serialize() ([]byte, error) {
	res, err := yaml.Marshal(base)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// deserialize tries to convert provided data into a baseConfig
func (base *BaseConfig) deserialize(data []byte) error {
	err := yaml.Unmarshal(data, &base)
	if err != nil {
		return err
	}
	return nil
}

// validate Validates baseConfig
func (base BaseConfig) validate() error {
	// check valid tags
	_, err := valid.ValidateStruct(base)
	if err != nil {
		return err
	}

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

// NDBConfig represents an ndb storage configuration
type NDBConfig struct {
	StorageCluster         StorageClusterConfig `yaml:"storageCluster" valid:"optional"`
	TemplateStorageCluster StorageClusterConfig `yaml:"templateStorageCluster" valid:"optional"`
	TemplateVdiskID        string               `yaml:"templateVdiskID" valid:"optional"`
}

// NewNDBConfig creates a new NDBConfig from byte slice in YAML 1.2 format
func NewNDBConfig(data []byte, vID string, vtype VdiskType) (*NDBConfig, error) {
	ndb := new(NDBConfig)
	err := ndb.deserialize(data)
	if err != nil {
		return nil, err
	}
	ndb.validate(vID, vtype)
	if err != nil {
		return nil, err
	}
	return ndb, nil
}

// serialize converts NDBConfig in byte slice in YAML 1.2 format
func (ndb *NDBConfig) serialize() ([]byte, error) {
	res, err := yaml.Marshal(ndb)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// deserialize tries to convert provided data into an NDBConfig
func (ndb *NDBConfig) deserialize(data []byte) error {
	err := yaml.Unmarshal(data, ndb)
	if err != nil {
		return err
	}
	return nil
}

// validate Validates NDBConfig
// Needs vdisk id and vdisk type
func (ndb NDBConfig) validate(vID string, vtype VdiskType) error {
	// check valid tags
	_, err := valid.ValidateStruct(ndb)
	if err != nil {
		return err
	}

	if ndb.StorageCluster.DataStorage == nil {
		return fmt.Errorf("no ndb datastorage was found")
	}

	// Check if templatestorage is present when required
	if vtype.TemplateSupport() {
		if ndb.TemplateStorageCluster.DataStorage == nil {
			return fmt.Errorf("template storage not found while required")
		}

		// nonDeduped vdisks that support templates,
		// also require a vdiskID as used on the template storage
		if vtype.StorageType() == StorageNonDeduped {
			if ndb.TemplateVdiskID == "" {
				log.Debugf("defaulting templateVdiskID of vdisk %s to %s", vID, vID)
				ndb.TemplateVdiskID = vID
			}
		}
	}

	// validate if metadata storage is defined when required
	metadataUndefined := ndb.StorageCluster.MetadataStorage == nil
	if metadataUndefined && vtype.StorageType() == StorageDeduped {
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
	err := tlog.deserialize(data)
	if err != nil {
		return nil, err
	}
	tlog.validate()
	if err != nil {
		return nil, err
	}
	return tlog, nil
}

// serialize converts TlogConfig in byte slice in YAML 1.2 format
func (tlog *TlogConfig) serialize() ([]byte, error) {
	res, err := yaml.Marshal(tlog)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// deserialize tries to convert provided data into an TlogConfig
func (tlog *TlogConfig) deserialize(data []byte) error {
	err := yaml.Unmarshal(data, tlog)
	if err != nil {
		return err
	}
	return nil
}

// validate Validates TlogConfig
func (tlog TlogConfig) validate() error {
	// check valid tags
	_, err := valid.ValidateStruct(tlog)
	if err != nil {
		return err
	}

	if tlog.TlogStorageCluster.DataStorage == nil {
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
	err := slave.deserialize(data)
	if err != nil {
		return nil, err
	}
	slave.validate()
	if err != nil {
		return nil, err
	}
	return slave, nil
}

// serialize converts SlaveConfig in byte slice in YAML 1.2 format
func (slave *SlaveConfig) serialize() ([]byte, error) {
	res, err := yaml.Marshal(slave)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// deserialize tries to convert provided data into an SlaveConfig
func (slave *SlaveConfig) deserialize(data []byte) error {
	err := yaml.Unmarshal(data, *slave)
	if err != nil {
		return err
	}
	return nil
}

// validate Validates SlaveConfig
func (slave *SlaveConfig) validate() error {
	// slave is optional, if nil, skip
	if slave.SlaveStorageCluster.DataStorage == nil && slave.SlaveStorageCluster.MetadataStorage == nil {
		return nil
	}
	// check valid tags
	_, err := valid.ValidateStruct(slave)
	if err != nil {
		return err
	}

	return nil
}

// TlogSupport returns whether or not the data of this vdisk
// has to send to the tlog server, to log its transactions.
func (vtype VdiskType) TlogSupport() bool {
	return vtype&propTlogSupport != 0
}

// TemplateSupport returns whether or not
// this vdisk supports a template server,
// to get the data in case the data isn't available on
// the normal (local) storage cluster.
func (vtype VdiskType) TemplateSupport() bool {
	return vtype&propTemplateSupport != 0 // || (cfg.Type == VdiskTypeBoot && cfg.TemplateStorageCluster != "")
}

// VdiskType represents the type of a vdisk,
// and each valid bit defines a property of the vdisk,
// and its the different collections of valid bits that defines
// each valid and unique type.
type VdiskType uint8

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
func (t VdiskType) StorageType() StorageType {
	if t&propDeduped != 0 {
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
func (t VdiskType) Validate() error {
	switch t {
	case VdiskTypeBoot, VdiskTypeDB, VdiskTypeCache, VdiskTypeTmp:
		return nil
	default:
		return fmt.Errorf("%v is an invalid vdisk type", t)
	}
}

// String returns the storage type as a string value
func (t VdiskType) String() string {
	switch t {
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
func (t *VdiskType) SetString(s string) error {
	switch s {
	case vdiskTypeBootStr:
		*t = VdiskTypeBoot
	case vdiskTypeDBStr:
		*t = VdiskTypeDB
	case vdiskTypeCacheStr:
		*t = VdiskTypeCache
	case vdiskTypeTmpStr:
		*t = VdiskTypeTmp
	default:
		return fmt.Errorf("%q is not a valid VdiskType", s)
	}
	return nil
}

// MarshalYAML implements yaml.Marshaler.MarshalYAML
func (t *VdiskType) MarshalYAML() (interface{}, error) {
	if s := t.String(); s != vdiskTypeNilStr {
		return s, nil
	}
	return nil, fmt.Errorf("%v is not a valid VdiskType", t)
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (t *VdiskType) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawType string
	err = unmarshal(&rawType)
	if err != nil {
		return fmt.Errorf("%q is not a valid VdiskType: %s", t, err)
	}

	// if bug in go-yaml that won't marchal VdiskType properly
	// rawtype should then be a uint8
	// bug is known: https://github.com/go-yaml/yaml/issues/134
	if uintType, err := strconv.ParseUint(rawType, 10, 8); err == nil {
		tBuf := VdiskType(uint8(uintType))
		err = tBuf.Validate()
		if err != nil {
			return err
		}
		t.SetString(tBuf.String())

		return nil
	}

	err = t.SetString(rawType)
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

// Clone this StorageClusterConfig into a new StorageClusterConfig.
func (cfg StorageClusterConfig) Clone() (clone StorageClusterConfig) {
	clone.DataStorage = make([]StorageServerConfig, len(cfg.DataStorage))
	copy(clone.DataStorage, cfg.DataStorage)
	if cfg.MetadataStorage != nil {
		metadataStorage := *cfg.MetadataStorage
		clone.MetadataStorage = &metadataStorage
	}
	return clone
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
