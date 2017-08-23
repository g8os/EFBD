package rest

import (

	"gopkg.in/validator.v2"
	"github.com/zero-os/0-stor/server/goraml/librairies/reservation"
)

type NamespacesNsidReservationsPostRespBody struct {
	DataAccessToken  string                  `json:"dataAccessToken" validate:"nonzero"`
	Reservation      reservation.Reservation `json:"reservation" validate:"nonzero"`
	ReservationToken string                  `json:"reservationToken" validate:"nonzero"`
}

func (s NamespacesNsidReservationsPostRespBody) Validate() error {

	return validator.Validate(s)
}
