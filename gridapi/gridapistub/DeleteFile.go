package gridapistub

import (
	"gopkg.in/validator.v2"
)

type DeleteFile struct {
	Path string `json:"path" validate:"nonzero"`
}

func (s DeleteFile) Validate() error {

	return validator.Validate(s)
}
