package gridapiclient

import (
	"net/http"
)

const (
	defaultBaseURI = ""
)

type G8OSStatelessGRID struct {
	client     http.Client
	AuthHeader string // Authorization header, will be sent on each request if not empty
	BaseURI    string
	common     service // Reuse a single struct instead of allocating one for each service on the heap.

	Nodes           *NodesService
	Storageclusters *StorageclustersService
	Vdisks          *VdisksService
}

type service struct {
	client *G8OSStatelessGRID
}

func NewG8OSStatelessGRID() *G8OSStatelessGRID {
	c := &G8OSStatelessGRID{
		BaseURI: defaultBaseURI,
		client:  http.Client{},
	}
	c.common.client = c

	c.Nodes = (*NodesService)(&c.common)
	c.Storageclusters = (*StorageclustersService)(&c.common)
	c.Vdisks = (*VdisksService)(&c.common)

	return c
}
