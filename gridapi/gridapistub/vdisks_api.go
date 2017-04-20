package gridapistub

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// VdisksAPI is API implementation of /vdisks root endpoint
type VdisksAPI struct {
	NonDedupedVdisks []string
}

// ListVdisks is the handler for GET /vdisks
// List vdisks
func (api VdisksAPI) ListVdisks(w http.ResponseWriter, r *http.Request) {
	var respBody []VdiskListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateNewVdisk is the handler for POST /vdisks
// Create a new vdisk, can be a copy from an existing vdisk
func (api VdisksAPI) CreateNewVdisk(w http.ResponseWriter, r *http.Request) {
	var reqBody VdiskCreate

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

// GetVdiskInfo is the handler for GET /vdisks/{vdiskid}
// Get vdisk information
func (api VdisksAPI) GetVdiskInfo(w http.ResponseWriter, r *http.Request) {
	vdiskID := mux.Vars(r)["vdiskid"]
	if vdiskID == "" {
		http.Error(w, "`vdiskid` is required", http.StatusBadRequest)
		return
	}
	var respBody Vdisk
	respBody.Blocksize = 4096
	respBody.Id = vdiskID
	respBody.Size = 20 // 20 GiB
	respBody.Storagecluster = "default"
	respBody.Type = EnumVdiskTypeboot
	respBody.ReadOnly = false

	for _, nonDedupedVdiskID := range api.NonDedupedVdisks {
		if nonDedupedVdiskID == vdiskID {
			respBody.Type = EnumVdiskTypedb
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}

// DeleteVdisk is the handler for DELETE /vdisks/{vdiskid}
// Delete Vdisk
func (api VdisksAPI) DeleteVdisk(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ResizeVdisk is the handler for POST /vdisks/{vdiskid}/resize
// Resize Vdisk
func (api VdisksAPI) ResizeVdisk(w http.ResponseWriter, r *http.Request) {
	var reqBody VdiskResize

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

// RollbackVdisk is the handler for POST /vdisks/{vdiskid}/rollback
// Rollback a vdisk to a previous state
func (api VdisksAPI) RollbackVdisk(w http.ResponseWriter, r *http.Request) {
	var reqBody VdiskRollback

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
