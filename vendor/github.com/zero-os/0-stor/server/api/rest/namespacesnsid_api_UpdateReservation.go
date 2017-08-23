package rest

import (
	"encoding/json"
	"github.com/zero-os/0-stor/server/goraml/librairies/reservation"
	"net/http"
)

// UpdateReservation is the handler for PUT /namespaces/{nsid}/reservations/{id}
// Renew an existing reservation
func (api NamespacesAPI) UpdateReservation(w http.ResponseWriter, r *http.Request) {
	var reqBody reservation.ReservationRequest

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

}
