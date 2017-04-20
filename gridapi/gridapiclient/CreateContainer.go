package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type CreateContainer struct {
	Filesystems    []string       `json:"filesystems,omitempty"`
	Flist          string         `json:"flist" validate:"nonzero"`
	HostNetworking bool           `json:"hostNetworking"`
	Hostname       string         `json:"hostname" validate:"nonzero"`
	Id             string         `json:"id" validate:"nonzero"`
	InitProcesses  []CoreSystem   `json:"initProcesses,omitempty"`
	Nics           []ContainerNIC `json:"nics,omitempty"`
	Ports          []string       `json:"ports,omitempty"`
	Storage        string         `json:"storage,omitempty"`
}

func (s CreateContainer) Validate() error {

	return validator.Validate(s)
}
