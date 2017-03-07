package storagebackendcontroller

import (
	"gopkg.in/validator.v2"
)

type Server struct {
	ConnectionString string `json:"ConnectionString" validate:"nonzero"`
	Type             string `json:"Type" validate:"nonzero"`
}

func (s Server) Validate() error {

	return validator.Validate(s)
}
