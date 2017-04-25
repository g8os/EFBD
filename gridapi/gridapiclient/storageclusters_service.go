package gridapiclient

import (
	"encoding/json"
	"net/http"
)

type StorageclustersService service

// List all running clusters
func (s *StorageclustersService) ListAllClusters(headers, queryParams map[string]interface{}) ([]string, *http.Response, error) {
	var u []string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/storageclusters", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Deploy New Cluster
func (s *StorageclustersService) DeployNewCluster(body ClusterCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/storageclusters", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get full Information about specific cluster
func (s *StorageclustersService) GetClusterInfo(label string, headers, queryParams map[string]interface{}) (Cluster, *http.Response, error) {
	var u Cluster

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/storageclusters/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kill cluster
func (s *StorageclustersService) KillCluster(label string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/storageclusters/"+label, headers, queryParams)
}
