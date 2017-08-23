package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameBanksPostReqBody struct {
	Type BankAccount `json:"type" validate:"nonzero"`
}

func (s UsersUsernameBanksPostReqBody) Validate() error {

	return validator.Validate(s)
}
