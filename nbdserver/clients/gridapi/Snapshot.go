package gridapi

import (
	"gopkg.in/validator.v2"
)

// Read only copy of the state of the filesystem at a certain time
type Snapshot struct {
	Name       string `json:"name" validate:"nonzero"`
	SizeOnDisk int    `json:"sizeOnDisk" validate:"nonzero"`
	Timestamp  int    `json:"timestamp" validate:"nonzero"`
}

func (s Snapshot) Validate() error {

	return validator.Validate(s)
}
