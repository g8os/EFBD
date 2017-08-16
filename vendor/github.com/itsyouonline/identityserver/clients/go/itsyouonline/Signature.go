package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Signature struct {
	Date      DateTime `json:"date" validate:"nonzero"`
	PublicKey string   `json:"publicKey" validate:"nonzero"`
	Signature string   `json:"signature" validate:"nonzero"`
	SignedBy  string   `json:"signedBy" validate:"nonzero"`
}

func (s Signature) Validate() error {

	return validator.Validate(s)
}
