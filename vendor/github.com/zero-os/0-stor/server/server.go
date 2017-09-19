package server

import (
	"net"

	pb "github.com/zero-os/0-stor/grpc_store"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/db/badger"
	"google.golang.org/grpc"

	log "github.com/Sirupsen/logrus"
)

// StoreServer defines a 0-stor server
type StoreServer interface {
	Listen(string) (string, error)
	Close()
	DB() db.DB
	Addr() string
}

type grpcServer struct {
	db         db.DB
	addr       string
	lis        net.Listener
	grpcServer *grpc.Server
}

// New creates a grpc server with given DB data & meta directory
// if authEnabled is false, JWT authentification is disabled
func New(data, meta string, authEnabled bool) (StoreServer, error) {
	db, err := badger.New(data, meta)
	if err != nil {
		return nil, err
	}
	return NewWithDB(db, authEnabled)
}

// NewWithDB creates a grpc server with given DB object
// if authEnabled is false, JWT authentification is disabled
func NewWithDB(db *badger.BadgerDB, authEnabled bool) (StoreServer, error) {
	if !authEnabled {
		disableAuth()
	}
	s := &grpcServer{
		db:         db,
		grpcServer: grpc.NewServer(),
	}

	pb.RegisterObjectManagerServer(s.grpcServer, NewObjectAPI(db))
	pb.RegisterNamespaceManagerServer(s.grpcServer, NewNamespaceAPI(db))
	//pb.RegisterReservationManagerServer(srv.GRPCServer(), &rpc.ReservationManager{db})

	return s, nil
}

// Listen listens to given addr.
// The server is going be to started as separate goroutine.
// It listen to random port if the given addr is empty
// or ended with ":0"
func (s *grpcServer) Listen(addr string) (string, error) {
	var err error
	s.lis, err = net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}

	go s.grpcServer.Serve(s.lis)
	s.addr = s.lis.Addr().String()

	return s.addr, nil

}

func (s *grpcServer) Close() {
	log.Infoln("stop grpc server")
	s.grpcServer.GracefulStop()
	log.Infoln("closing database")
	s.db.Close()
}

func (s *grpcServer) DB() db.DB {
	return s.db
}

func (s *grpcServer) Addr() string {
	return s.addr
}
