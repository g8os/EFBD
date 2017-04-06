package gridapi

import (
	"gopkg.in/validator.v2"
)

// Arguments for a core.system command
type CoreSystem struct {
	Args        []string `json:"args,omitempty"`
	Environment []string `json:"environment,omitempty"`
	Name        string   `json:"name" validate:"nonzero"`
	Pwd         string   `json:"pwd,omitempty"`
	Stdin       string   `json:"stdin,omitempty"`
}

func (s CoreSystem) Validate() error {

	return validator.Validate(s)
}
