package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/stats"
)

// ListObjects is the handler for GET /namespaces/{nsid}/objects
// List keys of the namespaces
func (api NamespacesAPI) ListObjects(w http.ResponseWriter, r *http.Request) { // page := req.FormValue("page")// per_page := req.FormValue("per_page")
	vars := mux.Vars(r)
	namespace := vars["nsid"]

	// increase rate counter
	go stats.IncrRead(namespace)

	mgr := manager.NewObjectManager(namespace, api.db)

	page, perPage, err := pagination(r)
	if err != nil {
		jsonError(w, fmt.Errorf("bad pagination params: %v", err), http.StatusBadRequest)
		return
	}

	start := (page-1)*perPage + 1
	count := perPage

	ids, err := mgr.List(start, count)

	if err != nil {
		jsonError(w, err, http.StatusInternalServerError)
		return
	}

	var respBody = make([]string, len(ids))
	for i := range ids {
		respBody[i] = string(ids[i])
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}

func pagination(r *http.Request) (page, perPage int, err error) {
	// Pagination
	pageParam := r.FormValue("page")
	perPageParam := r.FormValue("per_page")

	if pageParam == "" {
		pageParam = "1"
	}

	if perPageParam == "" {
		perPageParam = "20"
	}

	page, err = strconv.Atoi(pageParam)
	if err != nil {
		return
	}

	perPage, err = strconv.Atoi(perPageParam)
	if err != nil {
		return
	}

	return
}
