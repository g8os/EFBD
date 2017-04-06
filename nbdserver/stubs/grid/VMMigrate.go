package grid

import (
	"gopkg.in/validator.v2"
)

type VMMigrate struct {
	Nodeid string `json:"nodeid" validate:"nonzero"`
}

func (s VMMigrate) Validate() error {

	return validator.Validate(s)
}
