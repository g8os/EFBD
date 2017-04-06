package gridapi

import (
	"encoding/json"
	"net/http"
)

type VolumesService service

// Create a new volume, can be a copy from an existing volume
func (s *VolumesService) CreateNewVolume(volumecreate VolumeCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/volumes", &volumecreate, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get volume information
func (s *VolumesService) GetVolumeInfo(volumeid string, headers, queryParams map[string]interface{}) (Volume, *http.Response, error) {
	var u Volume

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/volumes/"+volumeid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete Volume
func (s *VolumesService) DeleteVolume(volumeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/volumes/"+volumeid, headers, queryParams)
}

// Resize Volume
func (s *VolumesService) ResizeVolume(volumeid string, volumeresize VolumeResize, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/volumes/"+volumeid+"/resize", &volumeresize, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Rollback a volume to a previous state
func (s *VolumesService) RollbackVolume(volumeid string, volumerollback VolumeRollback, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/volumes/"+volumeid+"/rollback", &volumerollback, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
