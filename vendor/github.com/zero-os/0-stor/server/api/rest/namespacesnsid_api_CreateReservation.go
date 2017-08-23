package rest

import (
	"encoding/json"
	"net/http"
	"github.com/zero-os/0-stor/server/goraml/librairies/reservation"
)

// CreateReservation is the handler for POST /namespaces/{nsid}/reservations
// Create a reservation for the given resource.
func (api NamespacesAPI) CreateReservation(w http.ResponseWriter, r *http.Request) {
	var reqBody reservation.ReservationRequest

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	var respBody NamespacesNsidReservationsPostRespBody
	json.NewEncoder(w).Encode(&respBody)
}
