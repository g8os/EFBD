package gridapistub

import (
	"gopkg.in/validator.v2"
)

type VolumeCreate struct {
	Blocksize          int                        `json:"blocksize" validate:"nonzero"`
	ReadOnly           bool                       `json:"readOnly,omitempty"`
	Size               int                        `json:"size" validate:"nonzero"`
	Storagecluster     string                     `json:"storagecluster,omitempty"`
	Templatevolume     string                     `json:"templatevolume,omitempty"`
	TlogStoragecluster string                     `json:"tlogStoragecluster,omitempty"`
	Volumetype         EnumVolumeCreateVolumetype `json:"volumetype" validate:"nonzero"`
}

func (s VolumeCreate) Validate() error {

	return validator.Validate(s)
}
