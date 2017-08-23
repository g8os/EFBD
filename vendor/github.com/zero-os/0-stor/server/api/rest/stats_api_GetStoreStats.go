package rest

import (
	"encoding/json"
	"net/http"
)

// GetStoreStats is the handler for GET /stats
// Return usage statistics about the whole store
func (api StatsAPI) GetStoreStats(w http.ResponseWriter, r *http.Request) {
	var respBody StoreStat
	json.NewEncoder(w).Encode(&respBody)
}
