package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VdiskCreate struct {
	Blocksize          int                 `json:"blocksize" validate:"nonzero"`
	Id                 string              `json:"id" validate:"nonzero"`
	ReadOnly           bool                `json:"readOnly,omitempty"`
	Size               int                 `json:"size" validate:"nonzero"`
	Storagecluster     string              `json:"storagecluster,omitempty"`
	Templatevdisk      string              `json:"templatevdisk,omitempty"`
	TlogStoragecluster string              `json:"tlogStoragecluster,omitempty"`
	Type               EnumVdiskCreateType `json:"type" validate:"nonzero"`
}

func (s VdiskCreate) Validate() error {

	return validator.Validate(s)
}
