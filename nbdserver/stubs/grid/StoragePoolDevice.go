package grid

import (
	"gopkg.in/validator.v2"
)

type StoragePoolDevice struct {
	Status EnumStoragePoolDeviceStatus `json:"status" validate:"nonzero"`
	Uuid   string                      `json:"uuid" validate:"nonzero"`
}

func (s StoragePoolDevice) Validate() error {

	return validator.Validate(s)
}
