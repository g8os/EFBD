package grid

import (
	"gopkg.in/validator.v2"
)

// Information about memory
type MemInfo struct {
	Active      int     `json:"active" validate:"nonzero"`
	Available   int     `json:"available" validate:"nonzero"`
	Buffers     int     `json:"buffers" validate:"nonzero"`
	Cached      int     `json:"cached" validate:"nonzero"`
	Free        int     `json:"free" validate:"nonzero"`
	Inactive    int     `json:"inactive" validate:"nonzero"`
	Total       int     `json:"total" validate:"nonzero"`
	Used        int     `json:"used" validate:"nonzero"`
	UsedPercent float64 `json:"usedPercent" validate:"nonzero"`
	Wired       int     `json:"wired" validate:"nonzero"`
}

func (s MemInfo) Validate() error {

	return validator.Validate(s)
}
