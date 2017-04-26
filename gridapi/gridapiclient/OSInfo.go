package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Information on the operating system
type OSInfo struct {
	BootTime             int64  `json:"bootTime" validate:"nonzero"`
	Hostname             string `json:"hostname" validate:"nonzero"`
	Os                   string `json:"os" validate:"nonzero"`
	Platform             string `json:"platform" validate:"nonzero"`
	PlatformFamily       string `json:"platformFamily" validate:"nonzero"`
	PlatformVersion      string `json:"platformVersion" validate:"nonzero"`
	Procs                int64  `json:"procs" validate:"nonzero"`
	Uptime               int64  `json:"uptime" validate:"nonzero"`
	VirtualizationRole   string `json:"virtualizationRole" validate:"nonzero"`
	VirtualizationSystem string `json:"virtualizationSystem" validate:"nonzero"`
}

func (s OSInfo) Validate() error {

	return validator.Validate(s)
}
