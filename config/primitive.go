package config

import (
	"github.com/pkg/errors"
)

// primitive.go
// defines all primitive types of the config pkg

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
		return errors.Errorf("%s is an invalid VdiskType", vdiskType)
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
		return errors.Errorf("%q is not a valid VdiskType", s)
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler.MarshalYAML
func (vdiskType VdiskType) MarshalYAML() (interface{}, error) {
	// ignore invalid vdisk
	return vdiskType.String(), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (vdiskType *VdiskType) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawType string
	err = unmarshal(&rawType)
	if err != nil {
		return errors.Wrapf(err, "%q is not a valid VdiskType", vdiskType)
	}

	err = vdiskType.SetString(rawType)
	return err
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

// StorageServerState represents the states a storage server can be in
type StorageServerState int8

// Different types of public storage server states.
// Negative values can be used for custom/private states.
const (
	StorageServerStateOnline StorageServerState = iota
	StorageServerStateOffline
	StorageServerStateRepair
	StorageServerStateRespread
	StorageServerStateRIP
	StorageServerStateUnknown StorageServerState = -1
)

// String returns the storage type as a string value
func (state StorageServerState) String() string {
	switch state {
	case StorageServerStateOnline:
		return storageServerStateOnlineStr
	case StorageServerStateOffline:
		return storageServerStateOfflineStr
	case StorageServerStateRepair:
		return storageServerStateRepairStr
	case StorageServerStateRespread:
		return storageServerStateRespreadStr
	case StorageServerStateRIP:
		return storageServerStateRIPStr
	default:
		return ""
	}
}

// SetString allows you to set this StorageServerState using
// the correct string representation.
func (state *StorageServerState) SetString(s string) error {
	switch s {
	case storageServerStateOnlineStr:
		*state = StorageServerStateOnline
	case storageServerStateOfflineStr:
		*state = StorageServerStateOffline
	case storageServerStateRepairStr:
		*state = StorageServerStateRepair
	case storageServerStateRespreadStr:
		*state = StorageServerStateRespread
	case storageServerStateRIPStr:
		*state = StorageServerStateRIP
	default:
		return errors.Errorf("%q is not a valid StorageServerState", s)
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler.MarshalYAML
func (state StorageServerState) MarshalYAML() (interface{}, error) {
	// ignore invalid StorageServerState
	str := state.String()
	if str == "" {
		return nil, nil
	}
	return str, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.UnmarshalYAML
func (state *StorageServerState) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var rawState string
	err = unmarshal(&rawState)
	if err != nil {
		return errors.Wrapf(err, "%q is not a valid StorageServerState", rawState)
	}

	err = state.SetString(rawState)
	return err
}

// The public storage server states as a string.
const (
	storageServerStateOnlineStr   = "online"
	storageServerStateOfflineStr  = "offline"
	storageServerStateRepairStr   = "repair"
	storageServerStateRespreadStr = "respread"
	storageServerStateRIPStr      = "rip"
)
