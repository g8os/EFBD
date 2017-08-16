package reservation

import (
	"github.com/zero-os/0-stor/client/goraml/goraml"
	"gopkg.in/validator.v2"
)

type Reservation struct {
	AdminId      string          `json:"adminId" validate:"regexp=^[a-zA-Z0-9]+$,nonzero"`
	Created      goraml.DateTime `json:"created" validate:"nonzero"`
	ExpireAt     goraml.DateTime `json:"expireAt" validate:"nonzero"`
	Id           string          `json:"id" validate:"regexp=^[a-zA-Z0-9]+$,nonzero"`
	SizeReserved float64         `json:"sizeReserved" validate:"min=1,multipleOf=1,nonzero"`
	SizeUsed     float64         `json:"sizeUsed" validate:"min=1,nonzero"`
	Updated      goraml.DateTime `json:"updated" validate:"nonzero"`
}

func (s Reservation) Validate() error {

	return validator.Validate(s)
}
