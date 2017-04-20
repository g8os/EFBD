package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type Container struct {
	ContainerId    int                 `json:"containerId" validate:"nonzero"`
	Filesystems    []string            `json:"filesystems" validate:"nonzero"`
	Flist          string              `json:"flist" validate:"nonzero"`
	HostNetworking bool                `json:"hostNetworking"`
	Hostname       string              `json:"hostname" validate:"nonzero"`
	Initprocesses  []CoreSystem        `json:"initprocesses" validate:"nonzero"`
	Nics           []ContainerNIC      `json:"nics" validate:"nonzero"`
	Ports          []string            `json:"ports" validate:"nonzero"`
	Status         EnumContainerStatus `json:"status" validate:"nonzero"`
	Storage        string              `json:"storage" validate:"nonzero"`
}

func (s Container) Validate() error {

	return validator.Validate(s)
}
