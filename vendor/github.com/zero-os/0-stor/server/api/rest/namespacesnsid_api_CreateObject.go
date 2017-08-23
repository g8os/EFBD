package rest

import (
	"encoding/json"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/stats"
)

// CreateObject is the handler for POST /namespaces/{nsid}/objects
// Set an object into the namespace
func (api NamespacesAPI) CreateObject(w http.ResponseWriter, r *http.Request) {
	var reqBody Object

	// increase request counter
	go stats.IncrWrite(mux.Vars(r)["nsid"])

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		log.Errorf("Error decoding object json: %v", err)
		jsonError(w, err, http.StatusBadRequest)
		return
	}

	if err := reqBody.Validate(); err != nil {
		jsonError(w, err, http.StatusBadRequest)
		return
	}

	mgr := manager.NewObjectManager(mux.Vars(r)["nsid"], api.db)
	refList := make([]string, len(reqBody.ReferenceList))
	if reqBody.ReferenceList == nil {
		reqBody.ReferenceList = []ReferenceID{}
	}

	for i := range reqBody.ReferenceList {
		refList[i] = string(reqBody.ReferenceList[i])
	}

	if err := mgr.Set([]byte(reqBody.Id), []byte(reqBody.Data), refList); err != nil {
		jsonError(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(&reqBody)
}
