package stor

import (
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	stormeta "github.com/zero-os/0-stor/client/meta"
)

// Delete tlog metadata and data of this vdisk
func (c *Client) Delete() error {
	storMetaCli, err := stormeta.NewClient(c.metaShards)
	if err != nil {
		return err
	}

	for wr := range c.Walk(0, tlog.TimeNowTimestamp()) {
		if err := c.deleteData(storMetaCli, wr.StorKey, wr.Meta, wr.RefList); err != nil {
			return err
		}
	}
	// delete first meta
	if err := c.metaCli.deleteMeta(c.firstMetaEtcdKey); err != nil {
		return err
	}
	// delete last meta
	return c.metaCli.deleteMeta(c.lastMetaEtcdKey)
}

func (c *Client) deleteData(storMetaCli *stormeta.Client, key []byte, md *stormeta.Meta, refList []string) error {
	if len(refList) == 1 {
		if refList[0] != c.vdiskID { // cheap strict checking, it should never happen
			log.Errorf("unexpected reference list `%v` for vdisk `%v`", refList, c.vdiskID)
			return nil
		}
		return c.storClient.DeleteWithMeta(md)
	}
	// remove meta
	if err := storMetaCli.Delete(string(key)); err != nil {
		return err
	}

	// remove reference, the API won't return error in case
	// our refList is not exist.
	// we don't add strict checking like in the case there is only one refList because:
	// - we shouldn't need it
	// - the checking is not really cheap (need to check in slice)
	return c.storClient.RemoveReferenceList(key, c.refList)
}
