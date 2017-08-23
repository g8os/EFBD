package client

import (
	"encoding/json"
	"net/http"
)

type StatsService service

// Return usage statistics about the whole store
func (s *StatsService) GetStoreStats(headers, queryParams map[string]interface{}) (StoreStat, *http.Response, error) {
	var u StoreStat

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/stats", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}
