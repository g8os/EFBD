package gridapistub

import (
	"gopkg.in/validator.v2"
)

type Bridge struct {
	Name    string           `json:"name" validate:"nonzero"`
	Setting string           `json:"setting" validate:"nonzero"`
	Status  EnumBridgeStatus `json:"status" validate:"nonzero"`
}

func (s Bridge) Validate() error {

	return validator.Validate(s)
}
