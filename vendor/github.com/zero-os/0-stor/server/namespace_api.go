package server

import (
	"golang.org/x/net/context"

	pb "github.com/zero-os/0-stor/grpc_store"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/stats"
)

var _ (pb.NamespaceManagerServer) = (*NamespaceAPI)(nil)

type NamespaceAPI struct {
	db db.DB
}

func NewNamespaceAPI(db db.DB) *NamespaceAPI {
	return &NamespaceAPI{
		db: db,
	}
}

func (api *NamespaceAPI) Get(ctx context.Context, req *pb.GetNamespaceRequest) (*pb.GetNamespaceReply, error) {
	label := req.GetLabel()

	if err := validateJWT(ctx, MethodAdmin, label); err != nil {
		return nil, err
	}

	mgr := manager.NewNamespaceManager(api.db)

	count, err := mgr.Count(label)
	if err != nil {
		return nil, err
	}
	read, write := stats.Rate(label)

	resp := &pb.GetNamespaceReply{
		Namespace: &pb.Namespace{
			Label: label,
			// SpaceAvailale: ,
			// SpaceUsed: ,
			ReadRequestPerHour:  read,
			WriteRequestPerHour: write,
			NrObjects:           int64(count) - 1,
		},
	}

	return resp, nil
}
