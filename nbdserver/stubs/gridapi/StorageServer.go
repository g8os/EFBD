package gridapi

import (
	"gopkg.in/validator.v2"
)

type StorageServer struct {
	Container string                  `json:"container" validate:"nonzero"`
	Id        int                     `json:"id" validate:"nonzero"`
	Ip        string                  `json:"ip" validate:"nonzero"`
	Port      int                     `json:"port" validate:"nonzero"`
	Status    EnumStorageServerStatus `json:"status" validate:"nonzero"`
}

func (s StorageServer) Validate() error {

	return validator.Validate(s)
}
