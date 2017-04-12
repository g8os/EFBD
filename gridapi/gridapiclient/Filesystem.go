package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// A filesystem living in a storage pool
type Filesystem struct {
	Mountpoint string `json:"mountpoint" validate:"nonzero"`
	Name       string `json:"name" validate:"nonzero"`
	Parent     string `json:"parent" validate:"nonzero"`
	Quota      int    `json:"quota" validate:"nonzero"`
	ReadOnly   bool   `json:"readOnly"`
	SizeOnDisk int    `json:"sizeOnDisk" validate:"nonzero"`
}

func (s Filesystem) Validate() error {

	return validator.Validate(s)
}
