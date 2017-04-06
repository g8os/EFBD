package grid

import (
	"gopkg.in/validator.v2"
)

type Cluster struct {
	DataStorage     []HAStorageServer    `json:"dataStorage" validate:"nonzero"`
	DriveType       EnumClusterDriveType `json:"driveType" validate:"nonzero"`
	Label           string               `json:"label" validate:"nonzero"`
	MetadataStorage []HAStorageServer    `json:"metadataStorage" validate:"nonzero"`
	Nodes           []string             `json:"nodes" validate:"nonzero"`
	Status          EnumClusterStatus    `json:"status" validate:"nonzero"`
}

func (s Cluster) Validate() error {

	return validator.Validate(s)
}
