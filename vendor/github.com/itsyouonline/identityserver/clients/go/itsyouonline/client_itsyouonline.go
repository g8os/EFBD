package itsyouonline

import (
	"net/http"
)

const (
	defaultBaseURI = "https://itsyou.online/api"
)

type Itsyouonline struct {
	client     http.Client
	AuthHeader string // Authorization header, will be sent on each request if not empty
	BaseURI    string
	common     service // Reuse a single struct instead of allocating one for each service on the heap.

	Organizations *OrganizationsService
	Users         *UsersService
}

type service struct {
	client *Itsyouonline
}

func NewItsyouonline() *Itsyouonline {
	c := &Itsyouonline{
		BaseURI: defaultBaseURI,
		client:  http.Client{},
	}
	c.common.client = c

	c.Organizations = (*OrganizationsService)(&c.common)
	c.Users = (*UsersService)(&c.common)

	return c
}
