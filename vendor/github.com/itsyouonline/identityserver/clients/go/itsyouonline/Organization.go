package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Organization struct {
	Dns              []string        `json:"dns" validate:"max=100,nonzero"`
	Globalid         string          `json:"globalid" validate:"min=3,max=150,regexp=^[a-z\d\-_\s]{3,150}$,nonzero"`
	Includes         []string        `json:"includes" validate:"max=100,nonzero"`
	Includesuborgsof []string        `json:"includesuborgsof" validate:"nonzero"`
	Members          []string        `json:"members" validate:"max=2000,nonzero"`
	Orgmembers       []string        `json:"orgmembers" validate:"nonzero"`
	Orgowners        []string        `json:"orgowners" validate:"nonzero"`
	Owners           []string        `json:"owners" validate:"max=20,nonzero"`
	PublicKeys       []string        `json:"publicKeys" validate:"max=20,nonzero"`
	Requiredscopes   []RequiredScope `json:"requiredscopes" validate:"max=20,nonzero"`
}

func (s Organization) Validate() error {

	return validator.Validate(s)
}
