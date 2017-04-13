package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type Volume struct {
	Blocksize          int                  `json:"blocksize" validate:"nonzero"`
	Id                 string               `json:"id" validate:"nonzero"`
	ReadOnly           bool                 `json:"readOnly,omitempty"`
	Size               int                  `json:"size" validate:"nonzero"`
	Status             EnumVolumeStatus     `json:"status" validate:"nonzero"`
	Storagecluster     string               `json:"storagecluster" validate:"nonzero"`
	TlogStoragecluster string               `json:"tlogStoragecluster" validate:"nonzero"`
	Volumetype         EnumVolumeVolumetype `json:"type" validate:"nonzero"`
}

func (s Volume) Validate() error {

	return validator.Validate(s)
}
