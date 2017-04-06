//Package stubs provides fake implementations for external components like ardb.
// This can be used to test the performance of the nbdserver itself or for development purposes
package stubs

import (
	"errors"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/g8os/blockstor/nbdserver/stubs/grid"
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

//NewGridAPIServer starts an HTTP server listening on a system-chosen port on the local loopback interface, for use in tests without an external grid api service.
// When finished, Close() should be called on the returned server
func NewGridAPIServer(arbdConnectionStrings string, nonDedupedVolumes []string) (s *httptest.Server, url string, err error) {

	connectionStrings := strings.Split(arbdConnectionStrings, ",")
	if len(connectionStrings) < 2 {
		err = errors.New("At least two ardb connectionstrings should be given")
		return
	}
	r := mux.NewRouter()
	a := grid.StorageclustersAPI{}
	ss, err := connectionStringToStorageServer(connectionStrings[0])
	if err != nil {
		return
	}
	a.MetadataServer = grid.HAStorageServer{Master: ss}
	a.StorageServers = make([]grid.HAStorageServer, len(connectionStrings)-1)
	for i, c := range connectionStrings[1:] {
		if ss, err = connectionStringToStorageServer(c); err != nil {
			return
		}
		a.StorageServers[i] = grid.HAStorageServer{Master: ss}
	}

	grid.StorageclustersInterfaceRoutes(r, a)

	grid.VolumesInterfaceRoutes(r, grid.VolumesAPI{
		NonDedupedVolumes: nonDedupedVolumes,
	})

	s = httptest.NewServer(r)
	url = s.URL
	return
}

func connectionStringToStorageServer(connectionString string) (ss grid.StorageServer, err error) {
	ip, portstring, err := net.SplitHostPort(connectionString)
	if err != nil {
		return
	}
	ss.Ip = ip
	ss.Port, err = strconv.Atoi(portstring)
	return
}
