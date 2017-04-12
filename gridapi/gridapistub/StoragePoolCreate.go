package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Arguments to create a new storage pools
type StoragePoolCreate struct {
	DataProfile     EnumStoragePoolCreateDataProfile     `json:"dataProfile" validate:"nonzero"`
	Devices         []string                             `json:"devices" validate:"nonzero"`
	MetadataProfile EnumStoragePoolCreateMetadataProfile `json:"metadataProfile" validate:"nonzero"`
	Name            string                               `json:"name" validate:"nonzero"`
}

func (s StoragePoolCreate) Validate() error {

	return validator.Validate(s)
}
