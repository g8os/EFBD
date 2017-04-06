package gridapi

import (
	"gopkg.in/validator.v2"
)

type HAStorageServer struct {
	Master StorageServer `json:"master" validate:"nonzero"`
	Slave  StorageServer `json:"slave,omitempty"`
}

func (s HAStorageServer) Validate() error {

	return validator.Validate(s)
}
