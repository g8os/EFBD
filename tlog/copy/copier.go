package copy

import (
	"gopkg.in/validator.v2"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-stor/client/lib/hash"
	"github.com/zero-os/0-stor/client/meta"
)

type copier struct {
	sourceVdiskID string
	targetVdiskID string
	storCliSource *stor.Client
	storCliTarget *stor.Client
	hasher        *hash.Hasher
	confSource    config.Source
}

func newCopier(confSource config.Source, conf Config) (*copier, error) {
	if err := validator.Validate(conf); err != nil {
		return nil, err
	}
	storCliSource, err := stor.NewClientFromConfigSource(confSource, conf.SourceVdiskID, conf.PrivKey)
	if err != nil {
		return nil, err
	}

	storCliTarget, err := stor.NewClientFromConfigSource(confSource, conf.TargetVdiskID, conf.PrivKey)
	if err != nil {
		return nil, err
	}

	hasher, err := hash.NewHasher(hash.Config{
		Type: hash.TypeBlake2,
	})
	if err != nil {
		return nil, err
	}

	return &copier{
		sourceVdiskID: conf.SourceVdiskID,
		targetVdiskID: conf.TargetVdiskID,
		storCliSource: storCliSource,
		storCliTarget: storCliTarget,
		confSource:    confSource,
		hasher:        hasher,
	}, nil
}

func (c *copier) Copy() error {
	sourceTlogConf, err := config.ReadVdiskTlogConfig(c.confSource, c.sourceVdiskID)
	if err != nil {
		return err
	}

	targetTlogConf, err := config.ReadVdiskTlogConfig(c.confSource, c.targetVdiskID)
	if err != nil {
		return err
	}

	if targetTlogConf.ZeroStorClusterID != sourceTlogConf.ZeroStorClusterID {
		return c.copyFull()
	}
	return c.copyRef()
}

// copyFull copy both tlog data and metadata
func (c *copier) copyFull() error {
	var prevMd *meta.Meta

	for wr := range c.storCliSource.Walk(0, tlog.TimeNowTimestamp()) {
		md := wr.Meta

		// creates new key
		newKey := c.hasher.Hash(append([]byte(c.targetVdiskID), md.Key...))
		md.Key = newKey

		// link the key
		if prevMd != nil {
			md.Previous = prevMd.Key
			prevMd.Next = md.Key
		}

		md.Chunks = nil

		// store both meta & data
		if _, err := c.storCliTarget.Store(newKey, wr.Data, prevMd, md); err != nil {
			return err
		}
		prevMd = md
	}
	return c.storCliTarget.SetLastMetaKey(prevMd.Key)
}

// copyRef copy tlog metadata and only append the reflist of the data
func (c *copier) copyRef() error {
	var prevMd *meta.Meta

	for wr := range c.storCliSource.Walk(0, tlog.TimeNowTimestamp()) {
		md := wr.Meta

		// creates new key
		newKey := c.hasher.Hash(append([]byte(c.targetVdiskID), md.Key...))
		md.Key = newKey

		// build link
		if prevMd != nil {
			md.Previous = prevMd.Key
			prevMd.Next = newKey
		}

		// append ref list
		if err := c.storCliTarget.AppendRefList(md, []string{c.targetVdiskID}); err != nil {
			return err
		}

		// copy meta
		if err := c.storCliTarget.PutMeta(md.Key, md); err != nil {
			return err
		}

		if prevMd != nil {
			if err := c.storCliTarget.PutMeta(prevMd.Key, prevMd); err != nil {
				return err
			}
		} else {
			if err := c.storCliTarget.SetFirstMetaKey(md.Key); err != nil {
				return err
			}
		}

		prevMd = md
	}
	return c.storCliTarget.SetLastMetaKey(prevMd.Key)
}
