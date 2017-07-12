package delvdisk

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
)

func newNonDedupDelController(vdiskID string) delController {
	return &nonDedupDelController{vdiskID}
}

// nonDdedupDelController defines a delController implementation
// which can be used to delete the data for nondeduped content
type nonDedupDelController struct {
	vdiskID string
}

// BatchRequest implements delController.BatchRequest
func (c nonDedupDelController) BatchRequest(conn redis.Conn) error {
	log.Infof("deleting nondeduped data of vdisk %s...", c.vdiskID)
	err := conn.Send("DEL", ardb.NonDedupedStorageKey(c.vdiskID))
	if err != nil {
		return c.error(err)
	}

	return nil
}

// CheckRequest implements delController.CheckRequest
func (c nonDedupDelController) CheckRequest(conn redis.Conn) error {
	deleted, err := redis.Bool(conn.Receive())
	if err != nil {
		return c.error(err)
	}
	if deleted {
		return nil
	}

	return noDeleteError(fmt.Errorf(
		"no nondeduped data existed for vdisk %s", c.vdiskID))
}

func (c nonDedupDelController) error(err error) error {
	return fmt.Errorf("could not delete data of nondeduped content for vdisk %s: %s",
		c.vdiskID, err.Error())
}
