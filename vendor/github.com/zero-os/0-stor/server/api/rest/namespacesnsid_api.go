package rest

import (
	"encoding/json"
	"net/http"

	"github.com/zero-os/0-stor/server/db"
)

// NamespacesAPI is API implementation of /namespaces/{nsid} root endpoint
type NamespacesAPI struct {
	db db.DB
}

func NewNamespaceAPI(db db.DB) NamespacesInterface {
	return NamespacesAPI{db: db}
}

func jsonError(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Error{Error: err.Error()})
}
