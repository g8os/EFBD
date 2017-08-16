package reservation

import "gopkg.in/validator.v2"


type ReservationRequest struct {
	Period int64 `json:"period" validate:"min=1,nonzero"`
	Size   int64 `json:"size" validate:"min=1,nonzero"`
}

func (s ReservationRequest) Validate() error {

	return validator.Validate(s)
}
