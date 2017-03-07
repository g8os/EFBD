package storagebackendcontroller

import (
	"gopkg.in/validator.v2"
)

type StorageclusterClusternameGetRespBody struct {
	Metadataserver Server   `json:"metadataserver" validate:"nonzero"`
	Name           string   `json:"name" validate:"nonzero"`
	Storageservers []Server `json:"storageservers" validate:"nonzero"`
}

func (s StorageclusterClusternameGetRespBody) Validate() error {

	return validator.Validate(s)
}
