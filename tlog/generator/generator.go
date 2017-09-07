package generator

import (
	"fmt"

	"gopkg.in/validator.v2"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Config represent generator config
type Config struct {
	SourceVdiskID string `validate:"nonzero"`
	TargetVdiskID string `validate:"nonzero"`
	PrivKey       string `validate:"nonzero"`
	DataShards    int    `validate:"nonzero,min=1"`
	ParityShards  int    `validate:"nonzero,min=1"`
}

// Generator represents a tlog data generator/copier
type Generator struct {
	sourceVdiskID string
	flusher       *flusher.Flusher
	configSource  config.Source
}

// New creates new Generator
func New(configSource config.Source, conf Config) (*Generator, error) {
	if err := validator.Validate(conf); err != nil {
		return nil, err
	}

	flusher, err := flusher.New(configSource, conf.DataShards, conf.ParityShards, conf.TargetVdiskID, conf.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create flusher: %v", err)
	}
	return &Generator{
		sourceVdiskID: conf.SourceVdiskID,
		flusher:       flusher,
		configSource:  configSource,
	}, nil
}

// GenerateFromStorage generates tlog data from block storage
func (g *Generator) GenerateFromStorage() error {
	staticConf, err := config.ReadVdiskStaticConfig(g.configSource, g.sourceVdiskID)
	if err != nil {
		return err
	}

	storageConf, err := config.ReadNBDStorageConfig(g.configSource, g.sourceVdiskID, staticConf)
	if err != nil {
		return fmt.Errorf("failed to ReadNBDStorageConfig: %v", err)
	}

	indices, err := storage.ListBlockIndices(g.sourceVdiskID, staticConf.Type, &storageConf.StorageCluster)
	if err != nil {
		return fmt.Errorf("ListBlockIndices failed for vdisk `%v`: %v", g.sourceVdiskID, err)
	}

	ardbProv, err := ardb.StaticProvider(*storageConf, nil)
	if err != nil {
		return err
	}

	sourceStorage, err := storage.NewBlockStorage(storage.BlockStorageConfig{
		VdiskID:         g.sourceVdiskID,
		TemplateVdiskID: staticConf.TemplateVdiskID,
		VdiskType:       staticConf.Type,
		BlockSize:       int64(staticConf.BlockSize),
	}, ardbProv)
	if err != nil {
		return err
	}
	defer sourceStorage.Close()

	var seq uint64
	for _, idx := range indices {
		content, err := sourceStorage.GetBlock(idx)
		if err != nil {
			return err
		}

		if err := g.flusher.AddTransaction(schema.OpSet, seq, content, idx, tlog.TimeNowTimestamp()); err != nil {
			return err
		}
		seq++
	}
	_, err = g.flusher.Flush()
	log.Infof("GenerateFromStorage generates `%v` tlog data with err = %v", seq, err)
	return err
}

// CopyTlogData copy/fork tlog data
func (g *Generator) CopyTlogData() error {
	return nil
}
