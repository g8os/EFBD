package grid

import (
	"gopkg.in/validator.v2"
)

// A combination of block devices forming 1 logical storage unit.
type StoragePool struct {
	Capacity        int                            `json:"capacity" validate:"nonzero"`
	DataProfile     EnumStoragePoolDataProfile     `json:"dataProfile" validate:"nonzero"`
	MetadataProfile EnumStoragePoolMetadataProfile `json:"metadataProfile" validate:"nonzero"`
	Mountpoint      string                         `json:"mountpoint" validate:"nonzero"`
	Name            string                         `json:"name" validate:"nonzero"`
	Status          EnumStoragePoolStatus          `json:"status" validate:"nonzero"`
	TotalCapacity   int                            `json:"totalCapacity" validate:"nonzero"`
}

func (s StoragePool) Validate() error {

	return validator.Validate(s)
}
