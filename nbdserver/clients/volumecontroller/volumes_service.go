package volumecontroller

import (
	"encoding/json"
	"net/http"
)

type VolumesService service

// Create a new volume, can be a copy from an existing volume
func (s *VolumesService) CreateNewVolume(volumespostreqbody VolumesPostReqBody, headers, queryParams map[string]interface{}) (VolumesPostRespBody, *http.Response, error) {
	var u VolumesPostRespBody

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/volumes", &volumespostreqbody, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get volume information
func (s *VolumesService) GetVolumeInfo(volumeid string, headers, queryParams map[string]interface{}) (VolumeInformation, *http.Response, error) {
	var u VolumeInformation

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
func (s *VolumesService) ResizeVolume(volumeid string, volumesvolumeidresizepostreqbody VolumesVolumeidResizePostReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/volumes/"+volumeid+"/resize", &volumesvolumeidresizepostreqbody, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
