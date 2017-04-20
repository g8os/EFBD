package gridapistub

import (
	"gopkg.in/validator.v2"
)

type VdiskResize struct {
	NewSize int `json:"newSize" validate:"nonzero"`
}

func (s VdiskResize) Validate() error {

	return validator.Validate(s)
}
