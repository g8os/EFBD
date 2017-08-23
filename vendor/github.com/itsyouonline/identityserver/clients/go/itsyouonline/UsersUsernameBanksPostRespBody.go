package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameBanksPostRespBody struct {
	Type BankAccount `json:"type" validate:"nonzero"`
}

func (s UsersUsernameBanksPostRespBody) Validate() error {

	return validator.Validate(s)
}
