package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumesPostRespBody struct {
	Volumeid string `json:"volumeid" validate:"nonzero"`
}

func (s VolumesPostRespBody) Validate() error {

	return validator.Validate(s)
}
