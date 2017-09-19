package copy

import (
	"context"
	"fmt"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// generator represents a tlog data generator/copier
type generator struct {
	sourceVdiskID string
	targetVdiskID string
	flusher       *flusher.Flusher
	configSource  config.Source
	jobCount      int
}

// New creates new generator
func newGenerator(configSource config.Source, conf Config) (*generator, error) {
	flusher, err := flusher.New(configSource, conf.DataShards, conf.ParityShards, conf.TargetVdiskID, conf.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create flusher: %v", err)
	}
	return &generator{
		sourceVdiskID: conf.SourceVdiskID,
		targetVdiskID: conf.TargetVdiskID,
		flusher:       flusher,
		configSource:  configSource,
		jobCount:      conf.JobCount,
	}, nil
}

// GenerateFromStorage generates tlog data from block storage
func (g *generator) GenerateFromStorage(parentCtx context.Context) error {
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

	type idxContent struct {
		idx     int64
		content []byte
	}
	var (
		wg              sync.WaitGroup
		indicesCh       = make(chan int64, g.jobCount)
		idxContentCh    = make(chan idxContent, g.jobCount)
		errCh           = make(chan error)
		doneCh          = make(chan struct{})
		ctx, cancelFunc = context.WithCancel(parentCtx)
	)
	defer cancelFunc()

	// produces the indices we want to fetch
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, idx := range indices {
			select {
			case <-ctx.Done():
				return
			case indicesCh <- idx:
			}
		}
		close(indicesCh)
	}()

	// fetch the indices
	for i := 0; i < g.jobCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indicesCh {
				select {
				case <-ctx.Done():
					return
				default:
					content, err := sourceStorage.GetBlock(idx)
					if err != nil {
						errCh <- err
						return
					}
					idxContentCh <- idxContent{
						idx:     idx,
						content: content,
					}

				}
			}
		}()
	}

	// add to flusher
	var seq uint64
	wg.Add(1)
	go func() {
		timestamp := tlog.TimeNowTimestamp()
		defer wg.Done()

		for ic := range idxContentCh {
			select {
			case <-ctx.Done():
				return
			default:
				err = g.flusher.AddTransaction(schema.OpSet, seq, ic.content, ic.idx, timestamp)
				if err != nil {
					errCh <- err
					return
				}
				seq++
				if int(seq) == len(indices) {
					return
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case err := <-errCh:
		return err
	case <-doneCh:
		// all is good
	}

	_, err = g.flusher.Flush()
	log.Infof("GenerateFromStorage generates `%v` tlog data with err = %v", len(indices), err)
	return err
}
