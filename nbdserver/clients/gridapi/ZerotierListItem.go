package gridapi

import (
	"gopkg.in/validator.v2"
)

// Zerotier details
type ZerotierListItem struct {
	Name   string                   `json:"name" validate:"nonzero"`
	Nwid   string                   `json:"nwid" validate:"nonzero"`
	Status string                   `json:"status" validate:"nonzero"`
	Type   EnumZerotierListItemType `json:"type" validate:"nonzero"`
}

func (s ZerotierListItem) Validate() error {

	return validator.Validate(s)
}
