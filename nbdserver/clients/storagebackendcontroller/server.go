package storagebackendcontroller

import (
	"gopkg.in/validator.v2"
)

type server struct {
	ConnectionInfo string `json:"connectionInfo" validate:"nonzero"`
	Type           string `json:"type" validate:"nonzero"`
}

func (s server) Validate() error {

	return validator.Validate(s)
}
