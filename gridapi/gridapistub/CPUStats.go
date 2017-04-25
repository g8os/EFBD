package gridapistub

import (
	"gopkg.in/validator.v2"
)

// cpu time stats of all cpus combined
type CPUStats struct {
	GuestNice float64 `json:"guestNice" validate:"nonzero"`
	Idle      float64 `json:"idle" validate:"nonzero"`
	IoWait    float64 `json:"ioWait" validate:"nonzero"`
	Irq       float64 `json:"irq" validate:"nonzero"`
	Nice      float64 `json:"nice" validate:"nonzero"`
	SoftIrq   float64 `json:"softIrq" validate:"nonzero"`
	Steal     float64 `json:"steal" validate:"nonzero"`
	Stolen    float64 `json:"stolen" validate:"nonzero"`
	System    float64 `json:"system" validate:"nonzero"`
	User      float64 `json:"user" validate:"nonzero"`
}

func (s CPUStats) Validate() error {

	return validator.Validate(s)
}
