package storagebackendcontroller

import (
	"net/http"
)

const (
	defaultBaseURI = ""
)

type StorageBackend struct {
	client     http.Client
	AuthHeader string // Authorization header, will be sent on each request if not empty
	BaseURI    string
	common     service // Reuse a single struct instead of allocating one for each service on the heap.

	Storagecluster *StorageclusterService
}

type service struct {
	client *StorageBackend
}

func NewStorageBackend() *StorageBackend {
	c := &StorageBackend{
		BaseURI: defaultBaseURI,
		client:  http.Client{},
	}
	c.common.client = c

	c.Storagecluster = (*StorageclusterService)(&c.common)

	return c
}
