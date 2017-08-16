package rest

import (
	"net/http"
)

// objectsreferencesPut is the handler for PUT /namespaces/{nsid}/objects/references
// Update reference list.
// The reference list of the object will be update with the references from the request body
func (api NamespacesAPI) objectsreferencesPut(w http.ResponseWriter, r *http.Request) {
}
