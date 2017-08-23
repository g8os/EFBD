package contract

import (
	"encoding/json"
	"github.com/itsyouonline/identityserver/db/contract"
	"net/http"
)

type OrganizationsglobalidcontractsAPI struct {
}

// Get the contracts where the organization is 1 of the parties. Order descending by
// date.
// It is handler for GET /organizations/{globalid}/contracts
func (api OrganizationsglobalidcontractsAPI) Get(w http.ResponseWriter, r *http.Request) { // includeExpired := req.FormValue("includeExpired")// max := req.FormValue("max")// start := req.FormValue("start")
	var respBody contract.Contract
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}
