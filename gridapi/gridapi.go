package gridapi

import (
	"errors"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/g8os/blockstor/gridapi/gridapistub"
	"github.com/gorilla/mux"
)

//NewGridAPIServer starts an HTTP server listening on a system-chosen port on the local loopback interface, for use in tests without an external grid api service.
// When finished, Close() should be called on the returned server
func NewGridAPIServer(arbdConnectionStrings string, nonDedupedVdisks []string) (s *httptest.Server, url string, err error) {

	connectionStrings := strings.Split(arbdConnectionStrings, ",")
	if len(connectionStrings) < 2 {
		err = errors.New("At least two ardb connectionstrings should be given")
		return
	}
	r := mux.NewRouter()
	a := gridapistub.StorageclustersAPI{}
	ss, err := connectionStringToStorageServer(connectionStrings[0])
	if err != nil {
		return
	}
	a.MetadataServer = gridapistub.HAStorageServer{Master: ss}
	a.StorageServers = make([]gridapistub.HAStorageServer, len(connectionStrings)-1)
	for i, c := range connectionStrings[1:] {
		if ss, err = connectionStringToStorageServer(c); err != nil {
			return
		}
		a.StorageServers[i] = gridapistub.HAStorageServer{Master: ss}
	}

	gridapistub.StorageclustersInterfaceRoutes(r, a)

	gridapistub.VdisksInterfaceRoutes(r, gridapistub.VdisksAPI{
		NonDedupedVdisks: nonDedupedVdisks,
	})

	s = httptest.NewServer(r)
	url = s.URL
	return
}

func connectionStringToStorageServer(connectionString string) (ss gridapistub.StorageServer, err error) {
	ip, portstring, err := net.SplitHostPort(connectionString)
	if err != nil {
		return
	}
	ss.Ip = ip
	ss.Port, err = strconv.Atoi(portstring)
	return
}
