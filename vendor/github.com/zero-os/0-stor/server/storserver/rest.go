package storserver

import (
	"net"
	"net/http"
	"strings"

	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/routes"
)

var _ (StoreServer) = (*restServer)(nil)

// Server defines a 0-stor server
type restServer struct {
	db    db.DB
	addr  string
	route http.Handler
}

// NewRest creates an http server with given DB data & meta directory
func NewRest(data, meta string) (StoreServer, error) {
	db, err := badger.New(data, meta)
	if err != nil {
		return nil, err
	}

	return &restServer{
		route: routes.GetRouter(db),
		db:    db,
	}, nil
}

// Listen listens to given addr.
// The HTTP server is going be to started as separate goroutine.
// It listen to random port if the given addr is empty
// or ended with ":0"
func (s *restServer) Listen(addr string) (string, error) {
	// user specify the port
	// listen on that address
	if !strings.HasSuffix(addr, ":0") && addr != "" {
		go http.ListenAndServe(addr, s.route)
		s.addr = addr
		return addr, nil
	}

	// user doesn't specify the port
	// creates our own listener
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}

	go http.Serve(l, s.route)
	s.addr = l.Addr().String()

	return s.addr, nil
}

// Close releases all the resources
func (s *restServer) Close() {
	s.db.Close()
}

func (s *restServer) DB() db.DB {
	return s.db
}

func (s *restServer) Addr() string {
	return s.addr
}
