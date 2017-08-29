package itsyouonline

import (
	"fmt"
	"github.com/itsyouonline/identityserver/clients/go/itsyouonline/goraml"
	"gopkg.in/validator.v2"
)

type Contract struct {
	Content      string          `json:"content" validate:"nonzero"`
	ContractId   string          `json:"contractId" validate:"nonzero"`
	ContractType string          `json:"contractType" validate:"max=40,nonzero"`
	Expires      goraml.DateTime `json:"expires" validate:"nonzero"`
	Extends      []string        `json:"extends,omitempty" validate:"max=10"`
	Invalidates  []string        `json:"invalidates,omitempty" validate:"max=10"`
	Parties      []Party         `json:"parties" validate:"min=2,max=20,nonzero"`
	Signatures   []Signature     `json:"signatures" validate:"nonzero"`
}

func (s Contract) Validate() error {

	mParties := map[interface{}]struct{}{}
	for _, v := range s.Parties {
		mParties[v] = struct{}{}
	}
	if len(mParties) != len(s.Parties) {
		return fmt.Errorf("Parties must be unique")
	}

	return validator.Validate(s)
}
