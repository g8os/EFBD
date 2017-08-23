package client

import (
	"encoding/json"
	"net/http"

	"github.com/zero-os/0-stor/client/goraml/librairies/reservation"
)

type NamespacesService service

// Update reference list.
// The reference list of the object will be update with the references from the request body
func (s *NamespacesService) UpdateReferenceList(id, nsid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/namespaces/"+nsid+"/objects/"+id+"/references", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Delete object from the store
func (s *NamespacesService) DeleteObject(id, nsid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/namespaces/"+nsid+"/objects/"+id, headers, queryParams)
}

// Retrieve object from the store
func (s *NamespacesService) GetObject(id, nsid string, headers, queryParams map[string]interface{}) (Object, *http.Response, error) {
	var u Object

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/namespaces/"+nsid+"/objects/"+id, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List keys of the object in the namespace
func (s *NamespacesService) ListObjects(nsid string, headers, queryParams map[string]interface{}) ([]string, *http.Response, error) {
	var u []string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/namespaces/"+nsid+"/objects", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Set an object into the namespace
func (s *NamespacesService) CreateObject(nsid string, body Object, headers, queryParams map[string]interface{}) (Object, *http.Response, error) {
	var u Object

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/namespaces/"+nsid+"/objects", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Return information about a reservation
func (s *NamespacesService) NamespacesNsidReservationsIdGet(id, nsid string, body reservation.Reservation, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/namespaces/"+nsid+"/reservations/"+id, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Renew an existing reservation
func (s *NamespacesService) UpdateReservation(id, nsid string, body reservation.ReservationRequest, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/namespaces/"+nsid+"/reservations/"+id, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Return a list of all the existing reservation for the give resource
func (s *NamespacesService) ListReservations(nsid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/namespaces/"+nsid+"/reservations", headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Create a reservation for the given resource.
func (s *NamespacesService) CreateReservation(nsid string, body reservation.ReservationRequest, headers, queryParams map[string]interface{}) (NamespacesNsidReservationsPostRespBody, *http.Response, error) {
	var u NamespacesNsidReservationsPostRespBody

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/namespaces/"+nsid+"/reservations", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detail view about namespace
func (s *NamespacesService) GetNameSpace(nsid string, headers, queryParams map[string]interface{}) (Namespace, *http.Response, error) {
	var u Namespace

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/namespaces/"+nsid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}
