package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type BankAccount struct {
	Bic     string `json:"bic" validate:"max=11,nonzero"`
	Country string `json:"country" validate:"max=40,nonzero"`
	Iban    string `json:"iban" validate:"max=30,nonzero"`
	Label   Label  `json:"label" validate:"nonzero"`
}

func (s BankAccount) Validate() error {

	return validator.Validate(s)
}
