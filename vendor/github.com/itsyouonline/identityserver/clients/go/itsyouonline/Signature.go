package itsyouonline

import (
	"github.com/itsyouonline/identityserver/clients/go/itsyouonline/goraml"
	"gopkg.in/validator.v2"
)

type Signature struct {
	Date      goraml.DateTime `json:"date" validate:"nonzero"`
	PublicKey string          `json:"publicKey" validate:"nonzero"`
	Signature string          `json:"signature" validate:"nonzero"`
	SignedBy  string          `json:"signedBy" validate:"nonzero"`
}

func (s Signature) Validate() error {

	return validator.Validate(s)
}
