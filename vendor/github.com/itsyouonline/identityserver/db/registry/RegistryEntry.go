package registry

import (
	"gopkg.in/validator.v2"
)

//RegistryEntry is a key-pair in a user or organization registry
type RegistryEntry struct {
	Key   string `json:"Key" validate:"min=1,max=256,nonzero"`
	Value string `json:"Value" validate:"max=1024,nonzero"`
}

//Validate checks if the RegistryEntry is valid
func (s RegistryEntry) Validate() error {

	return validator.Validate(s)
}
