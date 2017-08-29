package itsyouonline

import (
	"github.com/itsyouonline/identityserver/clients/go/itsyouonline/goraml"
	"gopkg.in/validator.v2"
)

type KeyData struct {
	Algorithm string          `json:"algorithm" validate:"nonzero"`
	Comment   string          `json:"comment,omitempty"`
	Timestamp goraml.DateTime `json:"timestamp,omitempty"`
}

func (s KeyData) Validate() error {

	return validator.Validate(s)
}
