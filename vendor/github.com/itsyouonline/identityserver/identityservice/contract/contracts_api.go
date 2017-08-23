package contract

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	contractdb "github.com/itsyouonline/identityserver/db/contract"
	"net/http"
)

//ContractsAPI service
type ContractsAPI struct {
}

// Sign a contract
// It is handler for POST /contracts/{contractId}/signatures
func (api ContractsAPI) contractIdsignaturesPost(w http.ResponseWriter, r *http.Request) {
	contractID := mux.Vars(r)["contractId"]
	contractMngr := contractdb.NewManager(r)
	exists, err := contractMngr.Exists(contractID)
	if err != nil || !exists {
		if err != nil {
			log.Error(err)
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	var signature contractdb.Signature
	if err = json.NewDecoder(r.Body).Decode(&signature); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	err = contractMngr.AddSignature(contractID, signature)
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	http.Error(w, http.StatusText(http.StatusCreated), http.StatusCreated)
	return
}

// Get a contract
// contractIdGet is handler for GET /contracts/{contractId}
func (api ContractsAPI) contractIdGet(w http.ResponseWriter, r *http.Request) {
	contractID := mux.Vars(r)["contractId"]
	contractMngr := contractdb.NewManager(r)
	contract, err := contractMngr.Get(contractID)
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}

	json.NewEncoder(w).Encode(&contract)
}
