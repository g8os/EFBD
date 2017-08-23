package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type RegistryEntry struct {
	Key   string `json:"Key" validate:"min=1,max=256,nonzero"`
	Value string `json:"Value" validate:"max=1024,nonzero"`
}

func (s RegistryEntry) Validate() error {

	return validator.Validate(s)
}
