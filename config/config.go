package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	valid "github.com/asaskevich/govalidator"
	"github.com/go-yaml/yaml"
)

// ReadConfig reads the config from a given file
func ReadConfig(path string, user User) (*Config, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't create the config: %s", err.Error())
	}

	return FromBytes(bytes, user)
}

// FromBytes creates a config based on given YAML 1.2 content
func FromBytes(bytes []byte, user User) (*Config, error) {
	cfg := new(Config)

	// unmarshal the yaml content into our config,
	// which will give us basic validation guarantees
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return nil, err
	}

	// specify the user, as this is used for any future validation
	cfg.user = user

	// validate the config,
	// specific for the specified user
	err = cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("couldn't create the config: %s", err.Error())
	}

	return cfg, nil
}

// User defines the user of the config
type User uint8

// Supported Config Users
const (
	Global    User = NBDServer | TlogServer
	NBDServer User = 1 << iota
	TlogServer
)

// Config for the zerodisk backends
type Config struct {
	StorageClusters map[string]StorageClusterConfig `yaml:"storageClusters" valid:"required"`
	Vdisks          map[string]VdiskConfig          `yaml:"vdisks" valid:"required"`

	// information used to specialize validation of the config
	user User
}

// VdiskClusterConfig combines the vdisk config
// and its cluster configs
type VdiskClusterConfig struct {
	Vdisk       VdiskConfig
	DataCluster *StorageClusterConfig
	RootCluster *StorageClusterConfig
	TlogCluster *StorageClusterConfig
}

// SetUser sets the user,
// information which is used for any future validations of this config.
func (cfg *Config) SetUser(user User) {
	cfg.user = user
}

// Validate the Config to ensure that all required properties are present,
// and that all given properties are valid
func (cfg *Config) Validate() error {
	// validate generic requirements
	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return cfg.validationError(err)
	}

	// used to validate storage references
	storageClusters := make(map[string]struct{})
	for clusterID := range cfg.StorageClusters {
		storageClusters[clusterID] = struct{}{}
	}
	validRef := func(clusterID string) error {
		// validate existence
		if _, ok := storageClusters[clusterID]; !ok {
			return fmt.Errorf("invalid cluster reference %s", clusterID)
		}
		return nil
	}

	// optionally validate nbdserver specific properties
	if cfg.user&NBDServer != 0 {
		err = cfg.validateNBD(validRef)
		if err != nil {
			return cfg.validationError(err)
		}
	}

	// optionally validate tlogserver specific properties
	if cfg.user&TlogServer != 0 {
		err = cfg.validateTlog(validRef)
		if err != nil {
			return cfg.validationError(err)
		}
	}

	// config is valid
	return nil
}

// validateNBD tlogserver-specific content,
// to ensure the required properties are present,
// and that all given properties are valid.
func (cfg *Config) validateNBD(validRef func(string) error) error {
	var err error

	for vdiskID, vdisk := range cfg.Vdisks {
		// validate required properties
		if vdisk.BlockSize == 0 || vdisk.BlockSize%2 != 0 {
			return fmt.Errorf(
				"vdisk %s: %d is an invalid blockSize", vdiskID, vdisk.BlockSize)
		}
		if vdisk.Size == 0 {
			return fmt.Errorf(
				"vdisk %s: %d is an invalid size", vdiskID, vdisk.BlockSize)
		}
		if err = vdisk.Type.Validate(); err != nil {
			return fmt.Errorf(
				"vdisk %s: invalid type: %s", vdiskID, err.Error())
		}

		// validate (required) storage cluster
		if vdisk.StorageCluster == "" {
			return fmt.Errorf(
				"vdisk %s misses required storageCluster", vdiskID)
		}

		err = validRef(vdisk.StorageCluster)
		if err != nil {
			return fmt.Errorf(
				"vdisk %s misses required storageCluster: %s",
				vdiskID, err.Error())
		}
		err = cfg.validateMetadataStorage(vdisk.StorageCluster, &vdisk)
		if err != nil {
			return fmt.Errorf("invalid storageCluster for vdisk %s: %s", vdiskID, err)
		}

		// validate (optional) root storage cluster
		if vdisk.RootStorageCluster != "" {
			if err = validRef(vdisk.RootStorageCluster); err != nil {
				return fmt.Errorf("invalid rootStorageCluster for vdisk %s: %s", vdiskID, err)
			}
		}
	}

	return nil
}

// validateTlog tlogserver-specific content,
// to ensure the required properties are present,
// and that all given properties are valid.
func (cfg *Config) validateTlog(validRef func(string) error) (err error) {
	// validate tlog storage cluster
	for vdiskID, vdisk := range cfg.Vdisks {
		if vdisk.TlogStorageCluster == "" {
			return fmt.Errorf(
				"vdisk %s misses required tlogStorageCluster", vdiskID)
		}

		if err = validRef(vdisk.TlogStorageCluster); err != nil {
			err = fmt.Errorf(
				"vdisk %s misses required tlogStorageCluster: %s",
				vdiskID, err.Error())
			return
		}
	}

	return
}

// validationError returns a user-targetted validation message
func (cfg *Config) validationError(err error) error {
	switch cfg.user {
	case NBDServer:
		return fmt.Errorf("invalid nbdserver config: %s", err.Error())
	case TlogServer:
		return fmt.Errorf("invalid tlogserver config: %s", err.Error())
	default:
		return fmt.Errorf("invalid config: %s", err.Error())
	}
}

// VdiskClusterConfig returns a VdiskClusterConfig
// for a given VdiskID if possible
// WARNING: make sure the config is valid before calling this method
func (cfg *Config) VdiskClusterConfig(vdiskID string) (*VdiskClusterConfig, error) {
	vdiskCfg, ok := cfg.Vdisks[vdiskID]
	if !ok {
		return nil, fmt.Errorf("no config found for vdisk %s", vdiskID)
	}

	config := &VdiskClusterConfig{Vdisk: vdiskCfg}

	if vdiskCfg.StorageCluster != "" {
		cluster := cfg.StorageClusters[vdiskCfg.StorageCluster].Clone()
		config.DataCluster = &cluster
	}

	if vdiskCfg.RootStorageCluster != "" {
		cluster := cfg.StorageClusters[vdiskCfg.RootStorageCluster].Clone()
		config.RootCluster = &cluster
	}

	if vdiskCfg.TlogStorageCluster != "" {
		cluster := cfg.StorageClusters[vdiskCfg.TlogStorageCluster].Clone()
		config.TlogCluster = &cluster
	}

	return config, nil
}

func (cfg *Config) validateMetadataStorage(clusterID string, vdisk *VdiskConfig) error {
	metadataStorageIsUndefined := cfg.StorageClusters[clusterID].MetadataStorage == nil
	if metadataStorageIsUndefined && vdisk.StorageType() == StorageDeduped {
		return errors.New("metadataStorage is required")
	}

	return nil
}

// String returns this config as a YAML marshalled string
func (cfg *Config) String() string {
	bytes, _ := yaml.Marshal(cfg)
	return string(bytes)
}

// StorageClusterConfig defines the config for a storageCluster
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
	return
}

// ParseCSStorageServerConfigStrings allows you to parse a slice of raw dial config strings.
// Dial Config Strings are a simple format used to specify ardb connection configs
// easily as a command line argument.
// The format is as follows: `<ip>:<port>[@<db_index>][,<ip>:<port>[@<db_index>]]`,
// where the db_index is optional, and you can give multiple configs by
// seperating them with a comma.
func ParseCSStorageServerConfigStrings(dialCSConfigString string) (configs []StorageServerConfig, err error) {
	if dialCSConfigString == "" {
		return nil, nil
	}
	dialConfigStrings := strings.Split(dialCSConfigString, ",")

	// convert all connection strings into ConnectionConfigs
	for _, dialConfigString := range dialConfigStrings {
		// remove whitespace around
		dialConfigString = strings.TrimSpace(dialConfigString)

		// trailing commas are allowed
		if dialConfigString == "" {
			continue
		}

		var cfg StorageServerConfig
		parts := strings.Split(dialConfigString, "@")
		if n := len(parts); n < 2 {
			cfg.Address = dialConfigString
		} else {
			cfg.Database, err = strconv.Atoi(parts[n-1])
			if err != nil {
				err = nil // ignore actual error
				n++       // not a valid database, thus probably part of address
			}
			cfg.Address = strings.Join(parts[:n-1], "@")
		}
		if !valid.IsDialString(cfg.Address) {
			err = fmt.Errorf("%s is not a valid storage address", cfg.Address)
			return
		}
		configs = append(configs, cfg)
	}

	return
}

// StorageServerConfig defines the config for a storage server
type StorageServerConfig struct {
	Address  string `yaml:"address" valid:"dialstring,required"`
	Database int    `yaml:"db" valid:"optional"`
}

// VdiskConfig defines the config for a vdisk
type VdiskConfig struct {
	BlockSize          uint64    `yaml:"blockSize" valid:"optional"`
	ReadOnly           bool      `yaml:"readOnly" valid:"optional"`
	Size               uint64    `yaml:"size" valid:"optional"`
	StorageCluster     string    `yaml:"storageCluster" valid:"optional"`
	RootStorageCluster string    `yaml:"rootStorageCluster" valid:"optional"`
	TlogStorageCluster string    `yaml:"tlogStorageCluster" valid:"optional"`
	TlogSlaveSync      bool      `yaml:"tlogSlaveSync" valid:"optional"` // true if tlog need to sync ardb slave
	Type               VdiskType `yaml:"type" valid:"optional"`
}

// StorageType returns the type of storage this vdisk uses
func (cfg *VdiskConfig) StorageType() StorageType {
	if cfg.Type&propDeduped != 0 {
		return StorageDeduped
	}

	// TODO: handle propPersistent flag
	// ignore the propPersistent flag for now,
	// and treat non-persistent and persistent memory,
	// both as persistent nondeduped storage
	return StorageNondeduped
}

// TlogSupport returns whether or not the data of this vdisk
// has to send to the tlog server, to log its transactions.
func (cfg *VdiskConfig) TlogSupport() bool {
	return cfg.Type&propTlogSupport != 0
}

// TemplateSupport returns whether or not
// this vdisk supports a template (root) server,
// to get the data in case the data isn't available on
// the normal (local) storage cluster.
// TODO: use this to add the option of template support,
//       both in the deduped as well as the nondeduped storage
//       see: https://github.com/zero-os/0-Disk/issues/223
func (cfg *VdiskConfig) TemplateSupport() bool {
	return cfg.Type&propTemplateSupport != 0
}

// VdiskType represents the type of a vdisk
type VdiskType uint8

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
func (t VdiskType) MarshalYAML() (interface{}, error) {
	if s := t.String(); s != "" {
		return s, nil
	}

	return nil, fmt.Errorf("%v is not a valid VdiskType", t)
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (t *VdiskType) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawType string
	err = unmarshal(&rawType)
	if err != nil {
		err = fmt.Errorf("%q is not a valid VdiskType: %s", t, err)
		return
	}

	err = t.SetString(rawType)
	return
}

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

// StorageType represents the type of storage of a vdisk
type StorageType uint8

// UInt8 returns the storage type as an uint8 value
func (st StorageType) UInt8() uint8 {
	return uint8(st)
}

// String returns the name of the storage type
func (st StorageType) String() string {
	switch st {
	case StorageDeduped:
		return "deduped"
	case StorageNondeduped:
		return "nondeduped"
	default:
		return "Unknown"
	}
}

// Different types of storage
const (
	StorageNil     StorageType = 0
	StorageDeduped StorageType = 1 << iota
	StorageNondeduped
)

// Vdisk Properties
const (
	propDeduped VdiskType = 1 << iota
	propPersistent
	propTemporary
	propTlogSupport
	propTemplateSupport
)

func init() {
	valid.SetFieldsRequiredByDefault(true)
}
