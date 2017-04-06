package gridapi

import (
	"gopkg.in/validator.v2"
)

type WriteFile struct {
	Contents string `json:"contents" validate:"nonzero"`
	Path     string `json:"path" validate:"nonzero"`
}

func (s WriteFile) Validate() error {

	return validator.Validate(s)
}
