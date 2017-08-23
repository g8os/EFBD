package rest

import (
	"encoding/base64"
	"fmt"
	"net/http"

	client "github.com/zero-os/0-stor/client/goraml"
	"github.com/zero-os/0-stor/client/goraml/librairies/reservation"
	"github.com/zero-os/0-stor/client/stor/common"
)

func encodeID(id []byte) string {
	return base64.URLEncoding.EncodeToString(id)
}

func decodeID(id string) ([]byte, error) {
	return base64.URLEncoding.DecodeString(id)
}

func encodeData(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func decodeData(data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(data)
}

// Client is 0-stor client.
// it use go-raml generated client underneath
type Client struct {
	client *client.The_0_Stor
	nsid   string // namespace ID
}

// NewClient creates Client
func NewClient(addr, org, namespace, iyoJWTToken string) *Client {
	client := client.NewThe_0_Stor()
	client.BaseURI = addr

	if iyoJWTToken != "" {
		client.AuthHeader = "Bearer " + iyoJWTToken
	}

	return &Client{
		client: client,
		nsid:   org + "_0stor_" + namespace,
	}
}

// NamespaceGet gets detail view about namespace
func (c *Client) NamespaceGet() (*common.Namespace, error) {
	ns, resp, err := c.client.Namespaces.GetNameSpace(c.nsid, nil, nil)

	if err := checkRespCode2xxAndErr(err, resp); err != nil {
		return nil, err
	}

	return newCommonNamespace(ns), nil
}

// ReservationList return a list of all the existing reservation
func (c *Client) ReservationList() ([]common.Reservation, error) {
	return nil, nil
}

// ReservationCreate creates a reservation.
// size is Storage size you want to reserve in MiB.
// period is number of days the reservation is valid
func (c *Client) ReservationCreate(size, period int64) (reserv *common.Reservation, reservToken string, dataToken string, err error) {
	req := reservation.ReservationRequest{
		Period: period,
		Size:   size,
	}

	// call func
	respObj, resp, err := c.client.Namespaces.CreateReservation(c.nsid, req, nil, nil)

	err = checkRespCode2xxAndErr(err, resp)
	if err != nil {
		return
	}

	reserv = newCommonReservation(respObj.Reservation)
	reservToken = respObj.ReservationToken
	dataToken = respObj.DataAccessToken
	return
}

// ReservationGet returns information about a reservation
func (c *Client) ReservationGet(id []byte) (*common.Reservation, error) {
	return nil, nil
}

// ReservationUpdate renew an existing reservation
func (c *Client) ReservationUpdate(id []byte, size, period int64) error {
	req := reservation.ReservationRequest{
		Period: period,
		Size:   size,
	}
	resp, err := c.client.Namespaces.UpdateReservation(encodeID(id), c.nsid, req, nil, nil)

	return checkRespCode2xxAndErr(err, resp)
}

// ObjectList lists keys of the object in the namespace
func (c *Client) ObjectList(page, perPage int) ([]string, error) {
	qp := map[string]interface{}{
		"page":     page,
		"per_page": perPage,
	}

	ids, resp, err := c.client.Namespaces.ListObjects(c.nsid, nil, qp)

	return ids, checkRespCode2xxAndErr(err, resp)
}

// ObjectCreate creates an object
func (c *Client) ObjectCreate(id, data []byte, refList []string) (*common.Object, error) {
	obj := client.Object{
		Id:   encodeID(id),
		Data: encodeData(data),
		//ReferenceList: refList,
	}

	obj, resp, err := c.client.Namespaces.CreateObject(c.nsid, obj, nil, nil)

	if err := checkRespCode2xxAndErr(err, resp); err != nil {
		return nil, err
	}

	return newCommonObject(id, data, obj.ReferenceList), nil
}

// ObjectGet retrieve object from the store
func (c *Client) ObjectGet(id []byte) (*common.Object, error) {
	obj, resp, err := c.client.Namespaces.GetObject(encodeID(id), c.nsid, nil, nil)

	if err := checkRespCode2xxAndErr(err, resp); err != nil {
		return nil, err
	}

	decodedData, err := decodeData(obj.Data)
	if err != nil {
		return nil, err
	}
	return newCommonObject(id, decodedData, obj.ReferenceList), nil
}

// ObjectDelete delete object from the store
func (c *Client) ObjectDelete(id []byte) error {
	resp, err := c.client.Namespaces.DeleteObject(encodeID(id), c.nsid, nil, nil)
	return checkRespCode2xxAndErr(err, resp)
}

// ObjectExist tests if an object with this id exists
func (c *Client) ObjectExist(id []byte) (bool, error) {
	return false, nil
}

// ReferenceUpdate updates reference list
func (c *Client) ReferenceUpdate(id []byte, refList []string) error {
	resp, err := c.client.Namespaces.UpdateReferenceList(encodeID(id), c.nsid, nil, nil)
	return checkRespCode2xxAndErr(err, resp)
}

func checkRespCode2xxAndErr(err error, resp *http.Response) error {
	return checkRespCodeErr(err, resp, 200, 300)
}

func checkRespCodeErr(err error, resp *http.Response, startCode, endCode int) error {
	if err != nil {
		return err
	}
	if !checkRespCode(resp, startCode, endCode) {
		return newRespCodeError(resp)
	}
	return nil
}
func newRespCodeError(resp *http.Response) error {
	return fmt.Errorf("invalid resp code : %v", resp.StatusCode)
}

func checkRespCode(resp *http.Response, start, end int) bool {
	return resp.StatusCode >= start && resp.StatusCode < end
}
