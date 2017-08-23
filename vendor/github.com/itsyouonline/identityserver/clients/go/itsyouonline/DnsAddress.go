package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type DnsAddress struct {
	Name string `json:"name" validate:"min=4,max=250,regexp=^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9](?:\.[a-zA-Z]{2,})+$,nonzero"`
}

func (s DnsAddress) Validate() error {

	return validator.Validate(s)
}
