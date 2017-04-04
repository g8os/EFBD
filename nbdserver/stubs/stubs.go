//Package stubs provides fake implementations for external components like ardb.
// This can be used to test the performance of the nbdserver itself or for development purposes
package stubs

import (
	"errors"
	"net/http/httptest"
	"strings"

	"github.com/g8os/blockstor/nbdserver/stubs/storagebackendcontroller"
	"github.com/g8os/blockstor/nbdserver/stubs/volumecontroller"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
)

//NewMemoryRedisConn creates a redis connection that stores everything in memory
func NewMemoryRedisConn() (conn redis.Conn) {
	return &MemoryRedis{
		values:  make(values),
		hvalues: make(map[interface{}]values),
	}
}

//NewVolumeControllerServer starts an HTTP server listening on a system-chosen port on the local loopback interface, for use in tests without an external volumecontroller service.
// When finished, Close() should be called on the returned server
func NewVolumeControllerServer(nonDedupedVolumes []string) (s *httptest.Server, url string) {
	r := mux.NewRouter()

	volumecontroller.VolumesInterfaceRoutes(r, volumecontroller.VolumesAPI{
		NonDedupedVolumes: nonDedupedVolumes,
	})
	s = httptest.NewServer(r)
	url = s.URL
	return
}

//NewStorageBackendServer starts an HTTP server listening on a system-chosen port on the local loopback interface, for use in tests without an external storage controller service.
// When finished, Close() should be called on the returned server
func NewStorageBackendServer(arbdConnectionStrings string) (s *httptest.Server, url string, err error) {
	connectionStrings := strings.Split(arbdConnectionStrings, ",")
	if len(connectionStrings) < 2 {
		err = errors.New("At least two ardb connectionstrings should be given")
		return
	}
	a := storagebackendcontroller.StorageclusterAPI{}
	a.MetadataServer = storagebackendcontroller.Server{ConnectionString: connectionStrings[0]}
	a.StorageServers = make([]storagebackendcontroller.Server, len(connectionStrings)-1)
	for i, c := range connectionStrings[1:] {
		a.StorageServers[i] = storagebackendcontroller.Server{ConnectionString: c}
	}
	r := mux.NewRouter()

	storagebackendcontroller.StorageclusterInterfaceRoutes(r, a)
	s = httptest.NewServer(r)
	url = s.URL
	return
}
