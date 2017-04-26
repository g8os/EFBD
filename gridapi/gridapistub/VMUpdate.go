package gridapistub

import (
	"gopkg.in/validator.v2"
)

type VMUpdate struct {
	Cpu    int         `json:"cpu" validate:"nonzero"`
	Disks  []VDiskLink `json:"disks" validate:"nonzero"`
	Memory int         `json:"memory" validate:"nonzero"`
	Nics   []NicLink   `json:"nics" validate:"nonzero"`
}

func (s VMUpdate) Validate() error {

	return validator.Validate(s)
}
