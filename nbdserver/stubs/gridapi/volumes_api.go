package gridapi

import (
	"encoding/json"
	"net/http"
)

// VolumesAPI is API implementation of /volumes root endpoint
type VolumesAPI struct {
}

// CreateNewVolume is the handler for POST /volumes
// Create a new volume, can be a copy from an existing volume
func (api VolumesAPI) CreateNewVolume(w http.ResponseWriter, r *http.Request) {
	var reqBody VolumeCreate

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

// GetVolumeInfo is the handler for GET /volumes/{volumeid}
// Get volume information
func (api VolumesAPI) GetVolumeInfo(w http.ResponseWriter, r *http.Request) {
	var respBody Volume
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteVolume is the handler for DELETE /volumes/{volumeid}
// Delete Volume
func (api VolumesAPI) DeleteVolume(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// RollbackVolume is the handler for POST /volumes/{volumeid}/rollback
// Rollback a volume to a previous state
func (api VolumesAPI) RollbackVolume(w http.ResponseWriter, r *http.Request) {
	var reqBody VolumeRollback

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

// ResizeVolume is the handler for POST /volumes/{volumeid}/resize
// Resize Volume
func (api VolumesAPI) ResizeVolume(w http.ResponseWriter, r *http.Request) {
	var reqBody VolumeResize

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
