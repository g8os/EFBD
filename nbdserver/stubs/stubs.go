//Package stubs provides fake implementations for external components like ardb.
// This can be used to test the performance of the nbdserver itself or for development purposes
package stubs

import (
	"net/http/httptest"

	"github.com/g8os/blockstor/nbdserver/stubs/volumecontroller"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
)

//NewMemoryRedisConn creates a redis connection that stores everything in memory
func NewMemoryRedisConn() (conn redis.Conn) {
	return &MemoryRedis{values: make(map[interface{}]interface{})}
}

//NewVolumeControllerServer starts an HTTP server listening on a system-chosen port on the local loopback interface, for use in tests without an external volumecontroller service.
// When finished, Close() should be called on the returned server
func NewVolumeControllerServer() (s *httptest.Server, url string) {
	r := mux.NewRouter()

	volumecontroller.VolumesInterfaceRoutes(r, volumecontroller.VolumesAPI{})
	s = httptest.NewServer(r)
	url = s.URL
	return
}
