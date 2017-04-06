package gridapi

import (
	"encoding/json"
	"net/http"
)

// StorageclustersAPI is API implementation of /storageclusters root endpoint
type StorageclustersAPI struct {
}

// ListAllClusters is the handler for GET /storageclusters
// List all running clusters
func (api StorageclustersAPI) ListAllClusters(w http.ResponseWriter, r *http.Request) {
	var respBody []string
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeployNewCluster is the handler for POST /storageclusters
// Deploy New Cluster
func (api StorageclustersAPI) DeployNewCluster(w http.ResponseWriter, r *http.Request) {
	var reqBody ClusterCreate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetClusterInfo is the handler for GET /storageclusters/{label}
// Get full Information about specific cluster
func (api StorageclustersAPI) GetClusterInfo(w http.ResponseWriter, r *http.Request) {
	var respBody Cluster
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillCluster is the handler for DELETE /storageclusters/{label}
// Kill cluster
func (api StorageclustersAPI) KillCluster(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}
