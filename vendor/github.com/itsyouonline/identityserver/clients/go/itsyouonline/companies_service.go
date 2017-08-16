package itsyouonline

import (
	"encoding/json"
	"net/http"
)

type CompaniesService service

// Get companies. Authorization limits are applied to requesting user.
func (s *CompaniesService) GetCompanyList(headers, queryParams map[string]interface{}) ([]Company, *http.Response, error) {
	var u []Company

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/companies", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Register a new company
func (s *CompaniesService) CreateCompany(company Company, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/companies", &company, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get organization info
func (s *CompaniesService) GetCompany(globalId string, headers, queryParams map[string]interface{}) (Company, *http.Response, error) {
	var u Company

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/companies/"+globalId, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update existing company. Updating ``globalId`` is not allowed.
func (s *CompaniesService) UpdateCompany(globalId string, headers, queryParams map[string]interface{}) (Company, *http.Response, error) {
	var u Company

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/companies/"+globalId, nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get the contracts where the organization is 1 of the parties. Order descending by date.
func (s *CompaniesService) GetCompanyContracts(globalId string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/companies/"+globalId+"/contracts", headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Create a new contract.
func (s *CompaniesService) CreateCompanyContract(globalId string, contract Contract, headers, queryParams map[string]interface{}) (Contract, *http.Response, error) {
	var u Contract

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/companies/"+globalId+"/contracts", &contract, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

func (s *CompaniesService) GetCompanyInfo(globalId string, headers, queryParams map[string]interface{}) (companyview, *http.Response, error) {
	var u companyview

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/companies/"+globalId+"/info", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

func (s *CompaniesService) CompaniesGlobalIdValidateGet(globalId string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/companies/"+globalId+"/validate", headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
