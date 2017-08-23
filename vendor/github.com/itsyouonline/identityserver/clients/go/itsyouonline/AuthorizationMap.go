package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// Mapping between requested labels and real labels
type AuthorizationMap struct {
	Reallabel      Label `json:"reallabel" validate:"nonzero"`
	Requestedlabel Label `json:"requestedlabel" validate:"nonzero"`
}

func (s AuthorizationMap) Validate() error {

	return validator.Validate(s)
}
