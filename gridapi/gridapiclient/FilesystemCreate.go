package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Arguments to create a new filesystem
type FilesystemCreate struct {
	Name     string `json:"name" validate:"nonzero"`
	Quota    int    `json:"quota" validate:"nonzero"`
	ReadOnly bool   `json:"readOnly"`
}

func (s FilesystemCreate) Validate() error {

	return validator.Validate(s)
}
