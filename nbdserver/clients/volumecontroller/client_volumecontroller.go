package volumecontroller

import (
	"net/http"
)

const (
	defaultBaseURI = ""
)

type VolumeController struct {
	client     http.Client
	AuthHeader string // Authorization header, will be sent on each request if not empty
	BaseURI    string
	common     service // Reuse a single struct instead of allocating one for each service on the heap.

	Volumes *VolumesService
}

type service struct {
	client *VolumeController
}

func NewVolumeController() *VolumeController {
	c := &VolumeController{
		BaseURI: defaultBaseURI,
		client:  http.Client{},
	}
	c.common.client = c

	c.Volumes = (*VolumesService)(&c.common)

	return c
}
