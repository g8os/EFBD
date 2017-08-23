package contract

import (
	"encoding/json"
	"github.com/itsyouonline/identityserver/db/contract"
	"net/http"
)

type UsersusernamecontractsAPI struct {
}

// Get the contracts where the user is 1 of the parties. Order descending by date.
// It is handler for GET /users/{username}/contracts
func (api UsersusernamecontractsAPI) Get(w http.ResponseWriter, r *http.Request) { // includeExpired := req.FormValue("includeExpired")// max := req.FormValue("max")// start := req.FormValue("start")
	var respBody contract.Contract
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}
