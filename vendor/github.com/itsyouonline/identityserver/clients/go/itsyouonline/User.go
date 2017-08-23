package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type User struct {
	Addresses      []Address             `json:"addresses" validate:"nonzero"`
	Bankaccounts   []BankAccount         `json:"bankaccounts" validate:"nonzero"`
	Digitalwallet  []DigitalAssetAddress `json:"digitalwallet" validate:"nonzero"`
	Emailaddresses []EmailAddress        `json:"emailaddresses" validate:"nonzero"`
	Expire         DateTime              `json:"expire,omitempty"`
	Facebook       FacebookAccount       `json:"facebook,omitempty"`
	Firstname      string                `json:"firstname" validate:"nonzero"`
	Github         GithubAccount         `json:"github,omitempty"`
	Lastname       string                `json:"lastname" validate:"nonzero"`
	Phonenumbers   []Phonenumber         `json:"phonenumbers" validate:"nonzero"`
	PublicKeys     []string              `json:"publicKeys" validate:"nonzero"`
	Username       string                `json:"username" validate:"min=2,max=30,regexp=^[a-z0-9]{2,30}$,nonzero"`
}

func (s User) Validate() error {

	return validator.Validate(s)
}
