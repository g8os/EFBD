package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Virtual machine list item
type VMListItem struct {
	Id     int                  `json:"id" validate:"nonzero"`
	Name   string               `json:"name" validate:"nonzero"`
	Status EnumVMListItemStatus `json:"status" validate:"nonzero"`
}

func (s VMListItem) Validate() error {

	return validator.Validate(s)
}
