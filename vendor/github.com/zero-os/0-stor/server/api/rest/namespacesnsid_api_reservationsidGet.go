package rest

import (
	"encoding/json"
	"github.com/zero-os/0-stor/server/goraml/librairies/reservation"
	"net/http"
)

// reservationsidGet is the handler for GET /namespaces/{nsid}/reservations/{id}
// Return information about a reservation
func (api NamespacesAPI) reservationsidGet(w http.ResponseWriter, r *http.Request) {
	var reqBody reservation.Reservation

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

}
