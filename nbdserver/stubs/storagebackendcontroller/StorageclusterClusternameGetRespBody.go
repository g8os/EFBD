package storagebackendcontroller

import (
	"gopkg.in/validator.v2"
)

type StorageclusterClusternameGetRespBody struct {
	Metadataserver server   `json:"metadataserver" validate:"nonzero"`
	Name           string   `json:"name" validate:"nonzero"`
	Storageservers []server `json:"storageservers" validate:"nonzero"`
}

func (s StorageclusterClusternameGetRespBody) Validate() error {

	return validator.Validate(s)
}
