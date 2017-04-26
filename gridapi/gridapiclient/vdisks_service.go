package gridapiclient

import (
	"encoding/json"
	"net/http"
)

type VdisksService service

// List vdisks
func (s *VdisksService) ListVdisks(headers, queryParams map[string]interface{}) ([]VdiskListItem, *http.Response, error) {
	var u []VdiskListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/vdisks", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new vdisk, can be a copy from an existing vdisk
func (s *VdisksService) CreateNewVdisk(body VdiskCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/vdisks", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get vdisk information
func (s *VdisksService) GetVdiskInfo(vdiskid string, headers, queryParams map[string]interface{}) (Vdisk, *http.Response, error) {
	var u Vdisk

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/vdisks/"+vdiskid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete Vdisk
func (s *VdisksService) DeleteVdisk(vdiskid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/vdisks/"+vdiskid, headers, queryParams)
}

// Resize Vdisk
func (s *VdisksService) ResizeVdisk(vdiskid string, body VdiskResize, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/vdisks/"+vdiskid+"/resize", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Rollback a vdisk to a previous state
func (s *VdisksService) RollbackVdisk(vdiskid string, body VdiskRollback, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/vdisks/"+vdiskid+"/rollback", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
