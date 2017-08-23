package rest

import (
	"net/http"
)

// ListReservations is the handler for GET /namespaces/{nsid}/reservations
// Return a list of all the existing reservation for the give resource
func (api NamespacesAPI) ListReservations(w http.ResponseWriter, r *http.Request) {
}
