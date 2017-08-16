package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// User specific API key
type UserAPIKey struct {
	Apikey        string   `json:"apikey" validate:"nonzero"`
	Applicationid string   `json:"applicationid" validate:"nonzero"`
	Label         Label    `json:"label" validate:"nonzero"`
	Scopes        []string `json:"scopes" validate:"nonzero"`
	Username      string   `json:"username" validate:"nonzero"`
}

func (s UserAPIKey) Validate() error {

	return validator.Validate(s)
}
