package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type userview struct {
	Addresses      []Address       `json:"addresses" validate:"nonzero"`
	Bankaccounts   []BankAccount   `json:"bankaccounts" validate:"nonzero"`
	Emailaddresses []EmailAddress  `json:"emailaddresses" validate:"nonzero"`
	Facebook       FacebookAccount `json:"facebook,omitempty"`
	Github         GithubAccount   `json:"github,omitempty"`
	Organizations  []string        `json:"organizations" validate:"nonzero"`
	Phonenumbers   []Phonenumber   `json:"phonenumbers" validate:"nonzero"`
	PublicKeys     []PublicKey     `json:"publicKeys,omitempty"`
	Username       string          `json:"username" validate:"nonzero"`
}

func (s userview) Validate() error {

	return validator.Validate(s)
}
