package storagebackendcontroller

import (
	"encoding/json"
	"net/http"
)

type StorageclusterService service

// Get storage cluster information
func (s *StorageclusterService) GetStorageClusterInfo(clustername string, headers, queryParams map[string]interface{}) (StorageclusterClusternameGetRespBody, *http.Response, error) {
	var u StorageclusterClusternameGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/storagecluster/"+clustername, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}
