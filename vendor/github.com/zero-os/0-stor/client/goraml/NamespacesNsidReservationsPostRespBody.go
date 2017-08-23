package client

import (
	"github.com/zero-os/0-stor/client/goraml/librairies/reservation"
	"gopkg.in/validator.v2"
)

type NamespacesNsidReservationsPostRespBody struct {
	DataAccessToken  string                  `json:"dataAccessToken" validate:"nonzero"`
	Reservation      reservation.Reservation `json:"reservation" validate:"nonzero"`
	ReservationToken string                  `json:"reservationToken" validate:"nonzero"`
}

func (s NamespacesNsidReservationsPostRespBody) Validate() error {

	return validator.Validate(s)
}
