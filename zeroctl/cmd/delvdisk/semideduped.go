package delvdisk

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/siddontang/go/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
)

func newSemiDedupDelController(vdiskID string) delController {
	return &semiDedupDelController{vdiskID}
}

// semiDedupDelController defines a delController implementation
// which can be used to delete the data for nondeduped content
type semiDedupDelController struct {
	vdiskID string
}

// BatchRequest implements delController.BatchRequest
func (c semiDedupDelController) BatchRequest(conn redis.Conn) error {
	log.Infof("deleting storage bitmap of semi deduped vdisk %s...", c.vdiskID)
	err := conn.Send("DEL", ardb.SemiDedupBitMapKey(c.vdiskID))
	if err != nil {
		return c.error(err)
	}

	return nil
}

// CheckRequest implements delController.CheckRequest
func (c semiDedupDelController) CheckRequest(conn redis.Conn) error {
	deleted, err := redis.Bool(conn.Receive())
	if err != nil {
		return c.error(err)
	}
	if deleted {
		return nil
	}

	return noDeleteError(fmt.Errorf(
		"no storage bitmap existed for semi deduped vdisk %s", c.vdiskID))
}

func (c semiDedupDelController) error(err error) error {
	return fmt.Errorf("could not delete storage bitmap for semi deduped vdisk %s: %s",
		c.vdiskID, err.Error())
}
