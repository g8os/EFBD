package contract

import (
	"encoding/json"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	contractdb "github.com/itsyouonline/identityserver/db/contract"
)

//CreateContract will save contract
func CreateContract(w http.ResponseWriter, r *http.Request, includedparty contractdb.Party) {
	contract := &contractdb.Contract{}
	if err := json.NewDecoder(r.Body).Decode(contract); err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if contract.ContractId == "" {
		log.Debug("ContractId can not be empty")
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	hasuser := false
	for _, party := range contract.Parties {
		if party.Type == includedparty.Type && party.Name == includedparty.Name {
			hasuser = true
		}
	}
	if !hasuser {
		contract.Parties = append(contract.Parties, includedparty)
	}
	contractMngr := contractdb.NewManager(r)
	err := contractMngr.Save(contract)
	if err != nil {
		log.Error("ERROR while saving contracts :\n", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(&contract)
}

//FindContracts for query
func FindContracts(w http.ResponseWriter, r *http.Request, includedparty contractdb.Party) {
	contractMngr := contractdb.NewManager(r)
	includeexpired := false
	if r.URL.Query().Get("includeExpired") == "true" {
		includeexpired = true
	}
	startstr := r.URL.Query().Get("start")
	start := 0
	var err error
	if startstr != "" {
		start, err = strconv.Atoi(startstr)
		if err != nil {
			log.Error("ERROR while parsing start :\n", err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
	}
	maxstr := r.URL.Query().Get("max")
	max := 50
	if maxstr != "" {
		max, err = strconv.Atoi(maxstr)
		if err != nil {
			log.Error("ERROR while parsing start :\n", err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
	}

	contracts, err := contractMngr.GetByIncludedParty(&includedparty, start, max, includeexpired)
	if err != nil {
		log.Error("ERROR while getting contracts :\n", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-type", "application/json")
	json.NewEncoder(w).Encode(&contracts)
}
