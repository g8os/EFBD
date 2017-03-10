package storagebackendcontroller

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// StorageclusterAPI is API implementation of /storagecluster root endpoint
type StorageclusterAPI struct {
	MetadataServer Server
	StorageServers []Server
}

// GetStorageClusterInfo is the handler for GET /storagecluster/{clustername}
// Get storage cluster information
func (api StorageclusterAPI) GetStorageClusterInfo(w http.ResponseWriter, r *http.Request) {
	var respBody StorageclusterClusternameGetRespBody

	respBody.Name = mux.Vars(r)["clustername"]
	respBody.Metadataserver = api.MetadataServer
	respBody.Storageservers = api.StorageServers

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}
