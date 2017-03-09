package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumeInformation struct {
	Blocksize      int    `json:"blocksize" validate:"nonzero"`
	Deduped        bool   `json:"deduped" validate:"nonzero"`
	Driver         string `json:"driver,omitempty"`
	Id             string `json:"id" validate:"nonzero"`
	ReadOnly       bool   `json:"readOnly,omitempty"`
	Size           int    `json:"size" validate:"nonzero"`
	Storagecluster string `json:"storagecluster" validate:"nonzero"`
}

func (s VolumeInformation) Validate() error {

	return validator.Validate(s)
}
