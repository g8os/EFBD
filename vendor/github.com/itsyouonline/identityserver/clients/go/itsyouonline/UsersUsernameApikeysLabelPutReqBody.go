package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameApikeysLabelPutReqBody struct {
	Label Label `json:"label" validate:"nonzero"`
}

func (s UsersUsernameApikeysLabelPutReqBody) Validate() error {

	return validator.Validate(s)
}
