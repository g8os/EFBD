package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameTwofamethodsGetRespBody struct {
	Sms  []Phonenumber `json:"sms" validate:"nonzero"`
	Totp bool          `json:"totp" validate:"nonzero"`
}

func (s UsersUsernameTwofamethodsGetRespBody) Validate() error {

	return validator.Validate(s)
}
