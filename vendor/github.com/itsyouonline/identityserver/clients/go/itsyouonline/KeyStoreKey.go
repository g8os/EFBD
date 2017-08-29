package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type KeyStoreKey struct {
	Globalid string  `json:"globalid,omitempty"`
	Key      string  `json:"key" validate:"nonzero"`
	Keydata  KeyData `json:"keydata" validate:"nonzero"`
	Label    Label   `json:"label" validate:"nonzero"`
	Username string  `json:"username,omitempty"`
}

func (s KeyStoreKey) Validate() error {

	return validator.Validate(s)
}
