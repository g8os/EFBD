package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameTotpGetRespBody struct {
	Totpissuer string `json:"totpissuer" validate:"nonzero"`
	Totpsecret string `json:"totpsecret" validate:"nonzero"`
}

func (s UsersUsernameTotpGetRespBody) Validate() error {

	return validator.Validate(s)
}
