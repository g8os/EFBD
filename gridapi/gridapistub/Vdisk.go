package gridapistub

import (
	"gopkg.in/validator.v2"
)

type Vdisk struct {
	Blocksize          int             `json:"blocksize" validate:"nonzero"`
	Id                 string          `json:"id" validate:"nonzero"`
	ReadOnly           bool            `json:"readOnly,omitempty"`
	Size               int             `json:"size" validate:"nonzero"`
	Status             EnumVdiskStatus `json:"status" validate:"nonzero"`
	Storagecluster     string          `json:"storagecluster" validate:"nonzero"`
	TlogStoragecluster string          `json:"tlogStoragecluster" validate:"nonzero"`
	Type               EnumVdiskType   `json:"type" validate:"nonzero"`
}

func (s Vdisk) Validate() error {

	return validator.Validate(s)
}
