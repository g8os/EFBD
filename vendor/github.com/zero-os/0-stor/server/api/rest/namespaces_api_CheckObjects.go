package rest

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/stats"
)

type checkRequest struct {
	Ids []string `json:"ids"`
}

// CheckObjects is the handler for GET /namespaces/{nsid}/check
// Check the status of some objects
// This command let you investigate the status of your data. It will validate that the data on disk has not been corrupted
func (api NamespacesAPI) CheckObjects(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["nsid"]

	// increase rate counter
	go stats.IncrRead(namespace)

	req := checkRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, err, http.StatusBadRequest)
		return
	}

	mgr := manager.NewObjectManager(namespace, api.db)

	var respBody = make([]CheckStatus, len(req.Ids))
	for i, id := range req.Ids {
		status, err := mgr.Check([]byte(id))
		if err != nil {
			jsonError(w, err, http.StatusInternalServerError)
			return
		}
		respBody[i] = CheckStatus{
			Id:     id,
			Status: EnumCheckStatusStatus(status),
		}
	}

	w.Header().Set("Content-type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}
