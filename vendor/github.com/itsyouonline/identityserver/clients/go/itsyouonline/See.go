package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// See object
type See struct {
	Globalid string       `json:"globalid" validate:"nonzero"`
	Uniqueid string       `json:"uniqueid" validate:"nonzero"`
	Username string       `json:"username" validate:"nonzero"`
	Versions []SeeVersion `json:"versions" validate:"nonzero"`
}

func (s See) Validate() error {

	return validator.Validate(s)
}
