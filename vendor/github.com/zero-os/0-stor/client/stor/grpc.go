package stor

import (
	"context"
	"fmt"
	"io"

	pb "github.com/zero-os/0-stor/grpc_store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ErrNotImplemented is the error return when a method is not implemented on the client
var ErrNotImplemented = fmt.Errorf("not implemented")

// client implement the stor.Client interface using grpc
type client struct {
	conn             *grpc.ClientConn
	objService       pb.ObjectManagerClient
	namespaceService pb.NamespaceManagerClient

	jwtToken  string
	namespace string
}

// New create a grpc client for the 0-stor
func newGrpcClient(conn *grpc.ClientConn, namespace, jwtToken string) *client {
	return &client{
		conn:             conn,
		objService:       pb.NewObjectManagerClient(conn),
		namespaceService: pb.NewNamespaceManagerClient(conn),
		jwtToken:         jwtToken,
		namespace:        namespace,
	}
}

func (c *client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *client) NamespaceGet() (*pb.Namespace, error) {
	resp, err := c.namespaceService.Get(ctxWithJWT(c.jwtToken), &pb.GetNamespaceRequest{Label: c.namespace})
	if err != nil {
		return nil, err
	}

	namespace := resp.GetNamespace()
	return namespace, nil
}

// func (c *client) ReservationList() (pb.Reservation, error) {
// 	return nil, ErrNotImplemented
// }

// func (c *client) ReservationCreate(size, period int64) (r *pb.Reservation, dataToken string, reservationToken string, err error) {
// 	return nil, "", "", ErrNotImplemented
// }

// func (c *client) ReservationGet(id []byte) (*pb.Reservation, error) {
// 	return nil, ErrNotImplemented
// }

// func (c *client) ReservationUpdate(id []byte, size, period int64) error {
// 	return ErrNotImplemented
// }

func (c *client) ObjectList(page, perPage int) ([]string, error) {
	stream, err := c.objService.List(ctxWithJWT(c.jwtToken), &pb.ListObjectsRequest{Label: c.namespace})
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, 100)

	for {
		obj, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		keys = append(keys, string(obj.GetKey()))
	}

	return keys, nil
}

func (c *client) ObjectCreate(id, data []byte, refList []string) error {
	_, err := c.objService.Create(ctxWithJWT(c.jwtToken), &pb.CreateObjectRequest{
		Label: c.namespace,
		Object: &pb.Object{
			Key:           id,
			Value:         data,
			ReferenceList: refList,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *client) ObjectGet(id []byte) (*pb.Object, error) {
	resp, err := c.objService.Get(ctxWithJWT(c.jwtToken), &pb.GetObjectRequest{
		Label: c.namespace,
		Key:   id,
	})
	if err != nil {
		return nil, err
	}

	obj := resp.GetObject()
	return obj, nil

}

func (c *client) ObjectDelete(id []byte) error {
	_, err := c.objService.Delete(ctxWithJWT(c.jwtToken), &pb.DeleteObjectRequest{
		Label: c.namespace,
		Key:   id,
	})

	return err
}

func (c *client) ObjectExist(id []byte) (bool, error) {
	resp, err := c.objService.Exists(ctxWithJWT(c.jwtToken), &pb.ExistsObjectRequest{
		Label: c.namespace,
		Key:   id,
	})

	return resp.GetExists(), err
}

func (c *client) ReferenceSet(id []byte, refList []string) error {
	_, err := c.objService.SetReferenceList(ctxWithJWT(c.jwtToken), &pb.UpdateReferenceListRequest{
		Label:         c.namespace,
		Key:           id,
		ReferenceList: refList,
	})

	return err
}

func (c *client) ReferenceAppend(id []byte, refList []string) error {
	_, err := c.objService.AppendReferenceList(ctxWithJWT(c.jwtToken), &pb.UpdateReferenceListRequest{
		Label:         c.namespace,
		Key:           id,
		ReferenceList: refList,
	})

	return err
}

func (c *client) ReferenceRemove(id []byte, refList []string) error {
	_, err := c.objService.RemoveReferenceList(ctxWithJWT(c.jwtToken), &pb.UpdateReferenceListRequest{
		Label:         c.namespace,
		Key:           id,
		ReferenceList: refList,
	})

	return err
}
func ctxWithJWT(jwt string) context.Context {
	md := metadata.Pairs("authorization", jwt)
	return metadata.NewOutgoingContext(context.Background(), md)
}
