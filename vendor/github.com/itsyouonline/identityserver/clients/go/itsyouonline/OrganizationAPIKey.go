package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationAPIKey struct {
	CallbackURL                string `json:"callbackURL,omitempty" validate:"max=250"`
	ClientCredentialsGrantType bool   `json:"clientCredentialsGrantType,omitempty"`
	Label                      Label  `json:"label" validate:"nonzero"`
	Secret                     string `json:"secret,omitempty" validate:"max=250"`
}

func (s OrganizationAPIKey) Validate() error {

	return validator.Validate(s)
}
