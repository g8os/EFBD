package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumeInformation struct {
	Blocksize      int    `json:"blocksize" validate:"nonzero"`
	Id             string `json:"id" validate:"nonzero"`
	Size           int    `json:"size" validate:"nonzero"`
	Storagecluster string `json:"storagecluster" validate:"nonzero"`
}

func (s VolumeInformation) Validate() error {

	return validator.Validate(s)
}
