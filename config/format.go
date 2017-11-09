package config

import (
	"fmt"

	valid "github.com/asaskevich/govalidator"
	"github.com/zero-os/0-Disk/errors"
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
	err := yaml.Unmarshal(data, &vdiskscfg)
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	err = vdiskscfg.Validate()
	if err != nil {
		return nil, NewInvalidConfigError(err)
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
	err := yaml.Unmarshal(data, &staticfg)
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	err = staticfg.Validate()
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	return staticfg, nil
}

// Validate implements FormatValidator.Validate.
func (cfg *NBDVdisksConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(ErrInvalidConfig,
			errors.Wrap(err, "invalid NBDVdisksConfig"))
	}

	return nil
}

// VdiskStaticConfig represents the static info of a vdisk.
type VdiskStaticConfig struct {
	BlockSize       uint64    `yaml:"blockSize" valid:"required"`
	ReadOnly        bool      `yaml:"readOnly" valid:"optional"`
	Size            uint64    `yaml:"size" valid:"required"`
	Type            VdiskType `yaml:"type" valid:"required"`
	TemplateVdiskID string    `yaml:"templateVdiskID" valid:"optional"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskStaticConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	// check valid tags
	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(
			ErrInvalidConfig, errors.Wrap(err, "invalid static config"))
	}

	// validate properties in more detail

	if !ValidateBlockSize(int64(cfg.BlockSize)) {
		return errors.Newf(
			"blockSize '%d' is invalid, should be equal-or-greater than 512 and be a power of 2",
			cfg.BlockSize,
		)
	}
	if (cfg.Size * gibibyteAsBytes) < cfg.BlockSize {
		return errors.Newf(
			"%d is an invalid size, has to be able to contain at least 1 block",
			cfg.Size,
		)
	}
	err = cfg.Type.Validate()
	if err != nil {
		return errors.Wrap(err, "VdiskStaticConfig has invalid type")
	}

	return nil
}

// NewVdiskNBDConfig creates a new VdiskNBDConfig from a given YAML slice.
func NewVdiskNBDConfig(data []byte) (*VdiskNBDConfig, error) {
	nbdcfg := new(VdiskNBDConfig)

	err := yaml.Unmarshal(data, nbdcfg)
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	err = nbdcfg.Validate()
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	return nbdcfg, nil
}

// VdiskNBDConfig represents the nbdserver-related information for a vdisk.
type VdiskNBDConfig struct {
	StorageClusterID         string `yaml:"storageClusterID" valid:"required"`
	TemplateStorageClusterID string `yaml:"templateStorageClusterID" valid:"optional"`
	SlaveStorageClusterID    string `yaml:"slaveStorageClusterID" valid:"optional"`
	TlogServerClusterID      string `yaml:"tlogServerClusterID" valid:"optional"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskNBDConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(ErrInvalidConfig,
			errors.Wrap(err, "invalid VdiskNBDConfig"))
	}

	return nil
}

// NewVdiskTlogConfig creates a new VdiskTlogConfig from a given YAML slice.
func NewVdiskTlogConfig(data []byte) (*VdiskTlogConfig, error) {
	tlogcfg := new(VdiskTlogConfig)

	err := yaml.Unmarshal(data, tlogcfg)
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
	ZeroStorClusterID string `yaml:"zeroStorClusterID" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *VdiskTlogConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(ErrInvalidConfig,
			errors.Wrap(err, "invalid VdiskTlogConfig"))
	}

	return nil
}

// NewStorageClusterConfig creates a new StorageClusterConfig from a given YAML slice.
func NewStorageClusterConfig(data []byte) (*StorageClusterConfig, error) {
	clustercfg := new(StorageClusterConfig)

	err := yaml.Unmarshal(data, clustercfg)
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	err = clustercfg.Validate()
	if err != nil {
		return nil, NewInvalidConfigError(err)
	}

	return clustercfg, nil
}

// StorageClusterConfig defines the config for a storageCluster.
// A storage cluster is composed out of multiple data storage servers,
// and a single (optional) metadata storage.
type StorageClusterConfig struct {
	Servers []StorageServerConfig `yaml:"servers" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *StorageClusterConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(ErrInvalidConfig,
			errors.Wrap(err, "invalid StorageClusterConfig"))
	}

	// validate all data server configs
	// and ensure that at least one data server is enabled
	for _, serverConfig := range cfg.Servers {
		err = serverConfig.Validate()
		if err != nil {
			return errors.Wrap(err, "invalid StorageClusterConfig")
		}
	}

	return nil
}

// Clone implements Cloner.Clone
func (cfg *StorageClusterConfig) Clone() (clone StorageClusterConfig) {
	if cfg == nil {
		return
	}

	clone.Servers = make([]StorageServerConfig, len(cfg.Servers))
	copy(clone.Servers, cfg.Servers)
	return
}

// Equal checks if the 2 configs are equal.
// Note that the order of data storage servers matters,
// as this order defines where vdisk's data will end up being.
func (cfg *StorageClusterConfig) Equal(other StorageClusterConfig) bool {
	// check if source cluster is given
	if cfg == nil {
		return false
	}

	// check if the data storage length is equal,
	// if not than the configs can't be equal
	if len(cfg.Servers) != len(other.Servers) {
		return false
	}
	// check if all data storages are equal
	for i := range cfg.Servers {
		if !cfg.Servers[i].Equal(other.Servers[i]) {
			return false
		}
	}

	// all data storages are equal
	return true
}

// NewZeroStorClusterConfig creates a new ZeroStorClusterConfig from a given YAML slice.
func NewZeroStorClusterConfig(data []byte) (*ZeroStorClusterConfig, error) {
	clustercfg := new(ZeroStorClusterConfig)

	err := yaml.Unmarshal(data, clustercfg)
	if err != nil {
		return nil, err
	}

	err = clustercfg.Validate()
	if err != nil {
		return nil, err
	}

	return clustercfg, nil
}

// ZeroStorClusterConfig defines the config for a ZeroStor server cluster
type ZeroStorClusterConfig struct {
	IYO             IYOCredentials `yaml:"iyo" valid:"required"`
	MetadataServers []ServerConfig `yaml:"metadataServers" valid:"required"`
	DataServers     []ServerConfig `yaml:"dataServers" valid:"required"`
	// data shards (K) variable of the erasure encoding
	DataShards int `yaml:"dataShards" valid:"required"`
	// parity shards (M) variable of the erasure encoding
	ParityShards int `yaml:"parityShards" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *ZeroStorClusterConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(ErrInvalidConfig,
			errors.Wrap(err, "invalid ZeroStorClusterConfig"))
	}

	if cfg.DataShards < 1 {
		return errors.Wrap(ErrInvalidConfig,
			"invalid ZeroStorClusterConfig: dataShards has to be at least 1")
	}
	if cfg.ParityShards < 1 {
		return errors.Wrap(ErrInvalidConfig,
			"invalid ZeroStorClusterConfig: parityShards has to be at least 1")
	}

	expectedServeCount := cfg.DataShards + cfg.ParityShards
	if len(cfg.DataServers) != expectedServeCount {
		return errors.Wrapf(
			ErrInvalidConfig,
			"invalid ZeroStorClusterConfig: expected %d (data+parity) servers, while %d servers were defined",
			expectedServeCount,
			len(cfg.DataServers),
		)
	}

	return nil
}

// Clone implements Cloner.Clone
func (cfg *ZeroStorClusterConfig) Clone() ZeroStorClusterConfig {
	var clone ZeroStorClusterConfig
	if cfg == nil {
		return clone
	}

	clone.IYO = cfg.IYO

	clone.MetadataServers = make([]ServerConfig, len(cfg.MetadataServers))
	copy(clone.MetadataServers, cfg.MetadataServers)

	clone.DataServers = make([]ServerConfig, len(cfg.DataServers))
	copy(clone.DataServers, cfg.DataServers)

	clone.DataShards = cfg.DataShards
	clone.ParityShards = cfg.ParityShards

	return clone
}

// Equal checks if the 2 configs are equal.
func (cfg *ZeroStorClusterConfig) Equal(other ZeroStorClusterConfig) bool {
	// check if source config is given
	if cfg == nil {
		return false
	}

	// compare data/parity shard count
	if cfg.DataShards != other.DataShards || cfg.ParityShards != other.ParityShards {
		return false
	}

	// check length Servers
	if len(cfg.DataServers) != len(other.DataServers) ||
		len(cfg.MetadataServers) != len(other.MetadataServers) {
		return false
	}

	// check if IYO credentials are equal
	if cfg.IYO != other.IYO {
		return false
	}

	// check if servers are equal
	for i := range cfg.DataServers {
		if cfg.DataServers[i] != other.DataServers[i] {
			return false
		}
	}
	for i := range cfg.MetadataServers {
		if cfg.MetadataServers[i] != other.MetadataServers[i] {
			return false
		}
	}

	return true
}

// IYOCredentials represents Itsyou.online credentials needed for 0-Stor namespacing
// More information about the namespacing of the 0-Stor:
// https://github.com/zero-os/0-stor/blob/master/specs/concept.md#namespaces-concept
type IYOCredentials struct {
	Org       string `yaml:"org" valid:"required"`
	Namespace string `yaml:"namespace" valid:"required"`
	ClientID  string `yaml:"clientID" valid:"optional"`
	Secret    string `yaml:"secret" valid:"optional"`
}

// NewTlogClusterConfig creates a new TlogClusterConfig from a given YAML slice.
func NewTlogClusterConfig(data []byte) (*TlogClusterConfig, error) {
	clustercfg := new(TlogClusterConfig)

	err := yaml.Unmarshal(data, clustercfg)
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
	Servers []string `yaml:"servers" valid:"required"`
}

// Validate implements FormatValidator.Validate.
func (cfg *TlogClusterConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}

	_, err := valid.ValidateStruct(cfg)
	if err != nil {
		return errors.WrapError(
			ErrInvalidConfig,
			errors.Wrap(err, "invalid TlogClusterConfig"))
	}

	err = isDialStringSlice(cfg.Servers)
	if err != nil {
		return err
	}

	return nil
}

// Clone implements Cloner.Clone
func (cfg *TlogClusterConfig) Clone() TlogClusterConfig {
	var clone TlogClusterConfig
	if cfg == nil || cfg.Servers == nil {
		return clone
	}

	clone.Servers = make([]string, len(cfg.Servers))
	copy(clone.Servers, cfg.Servers)
	return clone
}

// StorageServerConfig defines the config for a storage server
type StorageServerConfig struct {
	Address string `yaml:"address" valid:"optional"`
	// Database '0' is assumed, in case no value is given.
	Database int `yaml:"db" valid:"optional"`
	// State defines a storage server's state.
	// Depending on the state, the properties above become optional.
	State StorageServerState `yaml:"state" valid:"optional"`
}

// Validate this Storage Server Config,
// returning an error in case this config is invalid.
func (cfg *StorageServerConfig) Validate() error {
	if cfg == nil {
		return ErrNilConfig
	}
	if cfg.State == StorageServerStateRIP {
		return nil // nothing to validate here
	}

	if cfg.Database < 0 {
		return errors.Wrap(ErrInvalidConfig, "invalid database")
	}

	if cfg.Address == "" {
		return errors.Wrap(ErrInvalidConfig,
			"storage server address not given while it is required")
	}
	if !valid.IsDialString(cfg.Address) {
		return errors.Wrapf(ErrInvalidConfig,
			"storage server address '%s' is an invalid dialstring", cfg.Address)
	}

	// all checks out
	return nil
}

// Equal checks if the 2 configs are equal.
// Note that the order of data storage servers matters,
// as this order defines where vdisk's data will end up being.
func (cfg *StorageServerConfig) Equal(other StorageServerConfig) bool {
	if cfg == nil {
		return false
	}

	// are their states equal?!
	if cfg.State != other.State {
		return false
	}

	// last cheap test, are their databases equal?
	if cfg.Database != other.Database {
		return false
	}

	// it now all depends on whether or not their address is equal
	return cfg.Address == other.Address
}

// String implements Stringer.String
func (cfg StorageServerConfig) String() string {
	if cfg.Address == "" {
		switch cfg.State {
		case StorageServerStateRIP:
			return "✝"
		case StorageServerStateUnknown:
			return "?"
		default:
			return ""
		}
	}
	return fmt.Sprintf("%s@%d", cfg.Address, cfg.Database)
}

// isDialStringSlice checks a provided string slice
// if each element is a valid dial string.
func isDialStringSlice(data []string) error {
	for _, server := range data {
		if !valid.IsDialString(server) {
			return errors.Newf("%s is not a valid dial string", server)
		}
	}
	return nil
}

// ServerConfig represents a generic server config
// with only a service address.
type ServerConfig struct {
	Address string `yaml:"address" valid:"serviceaddress,required"`
}

const (
	// gibibyteAsBytes is a constant used to convert between GiB and bytes
	gibibyteAsBytes = 1024 * 1024 * 1024
)

func init() {
	valid.SetFieldsRequiredByDefault(true)
}
