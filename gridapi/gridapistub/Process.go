package gridapistub

import (
	"gopkg.in/validator.v2"
)

type Process struct {
	Cmd   Job     `json:"cmd" validate:"nonzero"`
	Cpu   float64 `json:"cpu" validate:"nonzero"`
	Debug string  `json:"debug" validate:"nonzero"`
	Name  string  `json:"name" validate:"nonzero"`
	Pid   int     `json:"pid" validate:"nonzero"`
	Rss   float64 `json:"rss" validate:"nonzero"`
	Swap  float64 `json:"swap" validate:"nonzero"`
	Vms   float64 `json:"vms" validate:"nonzero"`
}

func (s Process) Validate() error {

	return validator.Validate(s)
}
