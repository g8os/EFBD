package gridapistub

import (
	"gopkg.in/validator.v2"
)

type VdiskRollback struct {
	Epoch int `json:"epoch" validate:"nonzero"`
}

func (s VdiskRollback) Validate() error {

	return validator.Validate(s)
}
