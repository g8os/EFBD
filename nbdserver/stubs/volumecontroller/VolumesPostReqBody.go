package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumesPostReqBody struct {
	Blocksize      int    `json:"blocksize" validate:"nonzero"`
	Deduped        bool   `json:"deduped" validate:"nonzero"`
	Driver         string `json:"driver,omitempty"`
	ReadOnly       bool   `json:"readOnly,omitempty"`
	Size           int    `json:"size" validate:"nonzero"`
	Storagecluster string `json:"storagecluster" validate:"nonzero"`
	Templatevolume string `json:"templatevolume,omitempty"`
}

func (s VolumesPostReqBody) Validate() error {

	return validator.Validate(s)
}
