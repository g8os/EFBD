package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Definition of a virtual disk
type VDiskLink struct {
	MaxIOps          int    `json:"maxIOps" validate:"nonzero"`
	Storageclusterid string `json:"storageclusterid" validate:"nonzero"`
	Volumeid         string `json:"volumeid" validate:"nonzero"`
}

func (s VDiskLink) Validate() error {

	return validator.Validate(s)
}
