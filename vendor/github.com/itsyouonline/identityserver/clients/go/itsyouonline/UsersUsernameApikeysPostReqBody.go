package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameApikeysPostReqBody struct {
	Label Label `json:"label" validate:"nonzero"`
}

func (s UsersUsernameApikeysPostReqBody) Validate() error {

	return validator.Validate(s)
}
