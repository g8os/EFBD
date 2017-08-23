package rest

import (
	"time"

	goraml "github.com/zero-os/0-stor/client/goraml"
	"github.com/zero-os/0-stor/client/goraml/librairies/reservation"
	"github.com/zero-os/0-stor/client/stor/common"
)

// create common.Reservation from goraml.Reservation
func newCommonReservation(rv reservation.Reservation) *common.Reservation {
	return &common.Reservation{
		ID:           rv.Id,
		Created:      time.Time(rv.Created),
		ExpireAt:     time.Time(rv.ExpireAt),
		AdminID:      rv.AdminId,
		SizeReserved: rv.SizeReserved,
		SizeUsed:     rv.SizeUsed,
		Updated:      time.Time(rv.Updated),
	}
}

// creates common.Namespace from goraml.Namespace
func newCommonNamespace(ns goraml.Namespace) *common.Namespace {
	stat := ns.Stats
	return &common.Namespace{
		Label: ns.Label,
		Stats: common.NamespaceStat{
			NrObjects:           stat.NrObjects,
			ReadRequestPerHour:  stat.ReadRequestPerHour,
			SpaceAvailable:      stat.SpaceAvailable,
			SpaceUsed:           stat.SpaceUsed,
			WriteRequestPerHour: stat.WriteRequestPerHour,
		},
	}
}

// creates common.Object from goraml.Object
func newCommonObject(id, data []byte, refList []goraml.ReferenceID) *common.Object {
	obj := &common.Object{
		Key:   id,
		Value: data,
	}
	for _, ref := range refList {
		obj.ReferenceList = append(obj.ReferenceList, string(ref))
	}
	return obj
}
