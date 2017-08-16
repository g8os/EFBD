package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Company struct {
	Expire        DateTime `json:"expire,omitempty"`
	Globalid      string   `json:"globalid" validate:"min=3,max=150,regexp=^[a-z\d\-_\s]{3,150}$,nonzero"`
	Info          []string `json:"info,omitempty" validate:"max=20"`
	Organizations []string `json:"organizations,omitempty" validate:"max=100"`
	PublicKeys    []string `json:"publicKeys" validate:"max=20,nonzero"`
	Taxnr         string   `json:"taxnr,omitempty"`
}

func (s Company) Validate() error {

	return validator.Validate(s)
}
