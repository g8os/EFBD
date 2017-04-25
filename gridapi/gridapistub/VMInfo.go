package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Statistical information about a vm
type VMInfo struct {
	Cpu   []float64    `json:"cpu" validate:"nonzero"`
	Disks []VMDiskInfo `json:"disks" validate:"nonzero"`
	Nics  []VMNicInfo  `json:"nics" validate:"nonzero"`
}

func (s VMInfo) Validate() error {

	return validator.Validate(s)
}
