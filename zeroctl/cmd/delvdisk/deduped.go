package delvdisk

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

func newDedupDelController(vdiskID string) delController {
	return &dedupDelController{vdiskID}
}

// dedupDelController defines a delController implementation
// which can be used to delete the metadata for deduped content
type dedupDelController struct {
	vdiskID string
}

// BatchRequest implements delController.BatchRequest
func (c dedupDelController) BatchRequest(conn redis.Conn) error {
	log.Infof("deleting deduped metadata of vdisk %s...", c.vdiskID)
	err := conn.Send("DEL", lba.StorageKey(c.vdiskID))
	if err != nil {
		return c.error(err)
	}

	return nil
}

// CheckRequest implements delController.CheckRequest
func (c dedupDelController) CheckRequest(conn redis.Conn) error {
	deleted, err := redis.Bool(conn.Receive())
	if err != nil {
		return c.error(err)
	}
	if deleted {
		return nil
	}

	return noDeleteError(fmt.Errorf(
		"no deduped metadata existed for vdisk %s", c.vdiskID))
}

func (c dedupDelController) error(err error) error {
	return fmt.Errorf("could not delete metadata of deduped content for vdisk %s: %s",
		c.vdiskID, err.Error())
}
