package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernamePhonenumbersLabelValidatePutReqBody struct {
	Smscode       string `json:"smscode" validate:"nonzero"`
	Validationkey string `json:"validationkey" validate:"nonzero"`
}

func (s UsersUsernamePhonenumbersLabelValidatePutReqBody) Validate() error {

	return validator.Validate(s)
}
