package company

import (
	"encoding/json"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/itsyouonline/identityserver/db"
	companydb "github.com/itsyouonline/identityserver/db/company"
	contractdb "github.com/itsyouonline/identityserver/db/contract"
	"github.com/itsyouonline/identityserver/identityservice/contract"
)

type CompaniesAPI struct {
}

// Post is handler for POST /companies
// Register a new company
func (api CompaniesAPI) Post(w http.ResponseWriter, r *http.Request) {

	var company companydb.Company

	if err := json.NewDecoder(r.Body).Decode(&company); err != nil {
		log.Debug("Error decoding the company:", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	if !company.IsValid() {
		log.Debug("Invalid organization")
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	companyMgr := companydb.NewCompanyManager(r)
	err := companyMgr.Create(&company)
	if err != nil && err != db.ErrDuplicate {
		log.Error("Error saving company:", err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err == db.ErrDuplicate {
		log.Debug("Duplicate company:", company)
		http.Error(w, http.StatusText(http.StatusConflict), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(&company)
}

// globalIdPut is handler for PUT /companies/{globalId}
// Update existing company. Updating ``globalId`` is not allowed.
func (api CompaniesAPI) globalIdPut(w http.ResponseWriter, r *http.Request) {

	globalID := mux.Vars(r)["globalId"]

	var company companydb.Company

	if err := json.NewDecoder(r.Body).Decode(&company); err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	companyMgr := companydb.NewCompanyManager(r)

	oldCompany, cerr := companyMgr.GetByName(globalID)
	if cerr != nil {
		log.Debug(cerr)
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	if company.Globalid != globalID || company.GetId() != oldCompany.GetId() {
		http.Error(w, "Changing globalId or id is Forbidden!", http.StatusForbidden)
		return
	}

	if err := companyMgr.Save(&company); err != nil {
		log.Error("Error saving company:\n", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

}

// globalIdinfoGet is handler for GET /companies/{globalid}/info
func (api CompaniesAPI) globalIdinfoGet(w http.ResponseWriter, r *http.Request) {
	companyMgr := companydb.NewCompanyManager(r)

	globalID := mux.Vars(r)["globalId"]

	company, err := companyMgr.GetByName(globalID)
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	respBody := company

	w.Header().Set("Content-type", "application/json")
	json.NewEncoder(w).Encode(&respBody)
}

// globalIdvalidateGet It is handler for GET /companies/{globalid}/validate
func (api CompaniesAPI) globalIdvalidateGet(w http.ResponseWriter, r *http.Request) {
	log.Error("globalIdvalidateGet is not implemented")
}

// globalIdcontracts is handler for GET /companies/{globalId}/contracts
// Get the contracts where the organization is 1 of the parties. Order descending by date.
func (api CompaniesAPI) globalIdcontractsGet(w http.ResponseWriter, r *http.Request) {
	globalID := mux.Vars(r)["globalId"]
	includedparty := contractdb.Party{Type: "company", Name: globalID}
	contract.FindContracts(w, r, includedparty)
}

// RegisterNewContract is handler for GET /companies/{globalId}/contracts
func (api CompaniesAPI) RegisterNewContract(w http.ResponseWriter, r *http.Request) {
	globalID := mux.Vars(r)["glabalId"]
	includedparty := contractdb.Party{Type: "company", Name: globalID}
	contract.CreateContract(w, r, includedparty)
}

// GetCompanyList is the handler for GET /companies
// Get companies. Authorization limits are applied to requesting user.
func (api CompaniesAPI) GetCompanyList(w http.ResponseWriter, r *http.Request) {
	log.Error("GetCompanyList is not implemented")
}

// globalIdGet is the handler for GET /companies/{globalId}
// Get organization info
func (api CompaniesAPI) globalIdGet(w http.ResponseWriter, r *http.Request) {
	log.Error("globalIdGet is not implemented")
}
