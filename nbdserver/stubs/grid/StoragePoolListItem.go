package grid

import (
	"gopkg.in/validator.v2"
)

// A combination of block devices forming 1 logical storage unit.
type StoragePoolListItem struct {
	Capacity string                        `json:"capacity" validate:"nonzero"`
	Name     string                        `json:"name" validate:"nonzero"`
	Status   EnumStoragePoolListItemStatus `json:"status" validate:"nonzero"`
}

func (s StoragePoolListItem) Validate() error {

	return validator.Validate(s)
}
