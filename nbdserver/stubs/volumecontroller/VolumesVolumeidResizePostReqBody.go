package volumecontroller

import (
	"gopkg.in/validator.v2"
)

type VolumesVolumeidResizePostReqBody struct {
	NewSize int `json:"newSize" validate:"nonzero"`
}

func (s VolumesVolumeidResizePostReqBody) Validate() error {

	return validator.Validate(s)
}
