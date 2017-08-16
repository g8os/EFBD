package stor

import (
	"fmt"

	grpc0 "google.golang.org/grpc"

	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/stor/common"
	"github.com/zero-os/0-stor/client/stor/grpc"
	"github.com/zero-os/0-stor/client/stor/rest"
)

const (
	// ProtoRest specifies that we want to use stor client with REST protocol.
	// Because REST is not binary safe, the implementation need to encode/decode
	// the data and nsid using base64.
	// All decode/encode are handled by the implementation, user doesn't need
	// to worry about it
	ProtoRest = "rest"

	// ProtoGrpc specifies that we want to use stor client with GRPC protocol
	ProtoGrpc = "grpc"
)

// Client defines client interface to talk with 0-stor server
type Client interface {
	// Namespace gets detail view about namespace
	NamespaceGet() (*common.Namespace, error)

	// ReservationList return a list of all the existing reservation
	ReservationList() ([]common.Reservation, error)

	// ReservationCreate creates a reservation.
	// size is Storage size you want to reserve in MiB.
	// period is number of days the reservation is valid
	ReservationCreate(size, period int64) (r *common.Reservation, dataToken string, reservationToken string, err error)

	// ReservationGet returns information about a reservation
	ReservationGet(id []byte) (*common.Reservation, error)

	// ReservationUpdate renew an existing reservation
	ReservationUpdate(id []byte, size, period int64) error

	// ObjectList lists keys of the object in the namespace
	ObjectList(page, perPage int) ([]string, error)

	// ObjectCreate creates an object
	ObjectCreate(id, data []byte, refList []string) (*common.Object, error)

	// ObjectGet retrieve object from the store
	ObjectGet(id []byte) (*common.Object, error)

	// ObjectDelete delete object from the store
	ObjectDelete(id []byte) error

	// ObjectExist tests if an object with this id exists
	ObjectExist(id []byte) (bool, error)

	// ReferenceUpdate updates reference list
	ReferenceUpdate(id []byte, refList []string) error
}

// Config defines 0-stor client config
type Config struct {
	Protocol    string `yaml:"protocol"` // rest or grpc
	Shard       string `yaml:"shard"`    // 0-stor server address
	IyoClientID string `yaml:"iyo_client_id"`
	IyoSecret   string `yaml:"iyo_secret"`
}

// NewClient creates new 0-stor client
func NewClient(conf *Config, org, namespace string) (Client, error) {
	token, err := getIyoJWTToken(conf, org, namespace)
	if err != nil {
		return nil, err
	}
	return NewClientWithToken(conf, org, namespace, token)
}

// NewClientWithToken creates new client with the given token
func NewClientWithToken(conf *Config, org, namespace, iyoJWTToken string) (Client, error) {
	switch conf.Protocol {

	case ProtoRest:
		return rest.NewClient(conf.Shard, org, namespace, iyoJWTToken), nil

	case ProtoGrpc:
		conn, err := grpc0.Dial(conf.Shard, grpc0.WithInsecure())
		if err != nil {
			return nil, err
		}
		return grpc.New(conn, org, namespace, iyoJWTToken), nil

	default:
		return nil, fmt.Errorf("unsupported/invalid 0-stor protocol: %v", conf.Protocol)
	}
}

func getIyoJWTToken(conf *Config, org, namespace string) (string, error) {
	if conf.IyoSecret == "" || conf.IyoClientID == "" {
		return "", nil
	}

	iyoCli := itsyouonline.NewClient(org, conf.IyoClientID, conf.IyoSecret)
	return iyoCli.CreateJWT(namespace, itsyouonline.Permission{
		Admin:  true,
		Read:   true,
		Write:  true,
		Delete: true,
	})
}
