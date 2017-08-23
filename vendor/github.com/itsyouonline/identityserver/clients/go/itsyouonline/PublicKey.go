package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// PublicKey of a user
type PublicKey struct {
	Label     Label  `json:"label" validate:"nonzero"`
	Publickey string `json:"publickey" validate:"nonzero"`
}

func (s PublicKey) Validate() error {

	return validator.Validate(s)
}
