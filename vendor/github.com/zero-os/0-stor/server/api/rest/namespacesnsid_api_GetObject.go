package rest

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/stats"
)

// GetObject is the handler for GET /namespaces/{nsid}/objects/{id}
// Retrieve object from the server
func (api NamespacesAPI) GetObject(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	namespace := vars["nsid"]
	key := []byte(vars["id"])

	// increase rate counter
	go stats.IncrRead(namespace)

	mgr := manager.NewObjectManager(namespace, api.db)

	obj, err := mgr.Get(key)
	if err != nil {
		if err == db.ErrNotFound {
			jsonError(w, err, http.StatusNotFound)
		} else {
			jsonError(w, err, http.StatusInternalServerError)
		}
		return
	}

	resp := Object{
		Data:          string(obj.Data),
		Id:            vars["id"],
		ReferenceList: make([]ReferenceID, 0),
	}

	for i := range obj.ReferenceList {
		bRef := bytes.Trim(obj.ReferenceList[i][:], "\x00")
		if len(bRef) == 0 {
			continue
		}
		refid := ReferenceID(string(bRef))
		resp.ReferenceList = append(resp.ReferenceList, refid)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
