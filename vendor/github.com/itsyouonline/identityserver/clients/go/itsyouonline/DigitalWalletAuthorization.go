package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// Mapping between requested labels and real label. Also has a 'currency' property
type DigitalWalletAuthorization struct {
	Currency       string `json:"currency" validate:"min=1,max=15,nonzero"`
	Reallabel      Label  `json:"reallabel" validate:"nonzero"`
	Requestedlabel Label  `json:"requestedlabel" validate:"nonzero"`
}

func (s DigitalWalletAuthorization) Validate() error {

	return validator.Validate(s)
}
