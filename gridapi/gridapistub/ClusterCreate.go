package gridapistub

import (
	"gopkg.in/validator.v2"
)

type ClusterCreate struct {
	DriveType  EnumClusterCreateDriveType `json:"driveType" validate:"nonzero"`
	Label      string                     `json:"label" validate:"nonzero"`
	Nodes      []string                   `json:"nodes" validate:"nonzero"`
	Servers    int                        `json:"servers" validate:"nonzero"`
	SlaveNodes bool                       `json:"slaveNodes"`
}

func (s ClusterCreate) Validate() error {

	return validator.Validate(s)
}
