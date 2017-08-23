package itsyouonline

import (
	"encoding/json"
	"net/http"
)

type ContractsService service

// Get a contract
func (s *ContractsService) GetContract(contractId string, headers, queryParams map[string]interface{}) (Contract, *http.Response, error) {
	var u Contract

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/contracts/"+contractId, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Sign a contract
func (s *ContractsService) SignContract(contractId string, signature Signature, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/contracts/"+contractId+"/signatures", &signature, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
