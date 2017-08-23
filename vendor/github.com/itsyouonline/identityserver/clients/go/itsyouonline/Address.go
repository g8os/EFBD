package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Address struct {
	City       string `json:"city" validate:"max=30,nonzero"`
	Country    string `json:"country" validate:"max=40,nonzero"`
	Label      Label  `json:"label" validate:"nonzero"`
	Nr         string `json:"nr" validate:"max=10,nonzero"`
	Other      string `json:"other,omitempty" validate:"max=30"`
	Postalcode string `json:"postalcode" validate:"max=20,nonzero"`
	Street     string `json:"street" validate:"max=50,nonzero"`
}

func (s Address) Validate() error {

	return validator.Validate(s)
}
