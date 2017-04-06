package grid

import (
	"gopkg.in/validator.v2"
)

// Information about DiskInfo
type DiskInfo struct {
	Device     string           `json:"device" validate:"nonzero"`
	Fstype     string           `json:"fstype" validate:"nonzero"`
	Mountpoint string           `json:"mountpoint" validate:"nonzero"`
	Opts       string           `json:"opts" validate:"nonzero"`
	Size       int              `json:"size" validate:"nonzero"`
	Type       EnumDiskInfoType `json:"type" validate:"nonzero"`
}

func (s DiskInfo) Validate() error {

	return validator.Validate(s)
}
