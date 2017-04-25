package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VdiskListItem struct {
	Id             string                  `json:"id" validate:"nonzero"`
	Status         EnumVdiskListItemStatus `json:"status" validate:"nonzero"`
	Storagecluster string                  `json:"storagecluster" validate:"nonzero"`
	Type           EnumVdiskListItemType   `json:"type" validate:"nonzero"`
}

func (s VdiskListItem) Validate() error {

	return validator.Validate(s)
}
