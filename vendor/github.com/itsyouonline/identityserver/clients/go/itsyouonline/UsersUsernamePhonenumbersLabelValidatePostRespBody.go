package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernamePhonenumbersLabelValidatePostRespBody struct {
	Validationkey string `json:"validationkey" validate:"nonzero"`
}

func (s UsersUsernamePhonenumbersLabelValidatePostRespBody) Validate() error {

	return validator.Validate(s)
}
