package storagebackendcontroller

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// StorageclusterAPI is API implementation of /storagecluster root endpoint
type StorageclusterAPI struct {
}

// GetStorageClusterInfo is the handler for GET /storagecluster/{clustername}
// Get storage cluster information
func (api StorageclusterAPI) GetStorageClusterInfo(w http.ResponseWriter, r *http.Request) {
	var respBody StorageclusterClusternameGetRespBody
	lo := Server{}
	lo.Type = "ardb"
	lo.ConnectionString = "localhost:16379"

	respBody.Name = mux.Vars(r)["clustername"]
	respBody.Metadataserver = lo
	respBody.Storageservers = []Server{lo}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}