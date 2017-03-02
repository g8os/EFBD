package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumesPostReqBody struct {
	Size           int    `json:"size" validate:"nonzero"`
	Storagecluster string `json:"storagecluster" validate:"nonzero"`
	Templatevolume string `json:"templatevolume,omitempty"`
}

func (s VolumesPostReqBody) Validate() error {

	return validator.Validate(s)
}
