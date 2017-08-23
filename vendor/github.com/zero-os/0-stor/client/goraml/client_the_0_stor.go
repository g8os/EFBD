package client

import (
	"net/http"
)

const (
	defaultBaseURI = ""
)

type The_0_Stor struct {
	client     http.Client
	AuthHeader string // Authorization header, will be sent on each request if not empty
	BaseURI    string
	common     service // Reuse a single struct instead of allocating one for each service on the heap.

	Namespaces *NamespacesService
	Stats      *StatsService
}

type service struct {
	client *The_0_Stor
}

func NewThe_0_Stor() *The_0_Stor {
	c := &The_0_Stor{
		BaseURI: defaultBaseURI,
		client:  http.Client{},
	}
	c.common.client = c

	c.Namespaces = (*NamespacesService)(&c.common)
	c.Stats = (*StatsService)(&c.common)

	return c
}
