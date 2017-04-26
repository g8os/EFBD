package gridapistub

import (
	"gopkg.in/validator.v2"
)

type Process struct {
	Cmdline string   `json:"cmdline" validate:"nonzero"`
	Cpu     CPUStats `json:"cpu" validate:"nonzero"`
	Pid     int64    `json:"pid" validate:"nonzero"`
	Rss     int64    `json:"rss" validate:"nonzero"`
	Swap    int64    `json:"swap" validate:"nonzero"`
	Vms     int64    `json:"vms" validate:"nonzero"`
}

func (s Process) Validate() error {

	return validator.Validate(s)
}
