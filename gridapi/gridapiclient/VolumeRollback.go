package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VolumeRollback struct {
	Epoch int `json:"epoch" validate:"nonzero"`
}

func (s VolumeRollback) Validate() error {

	return validator.Validate(s)
}
