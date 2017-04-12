package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Statistical information about a vm
type VMInfo struct {
	Cpu  []float64    `json:"cpu" validate:"nonzero"`
	Disk []VMDiskInfo `json:"disk" validate:"nonzero"`
	Net  []VMNetInfo  `json:"net" validate:"nonzero"`
}

func (s VMInfo) Validate() error {

	return validator.Validate(s)
}
