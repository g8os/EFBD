package gridapi

import (
	"gopkg.in/validator.v2"
)

type VolumeResize struct {
	NewSize int `json:"newSize" validate:"nonzero"`
}

func (s VolumeResize) Validate() error {

	return validator.Validate(s)
}
