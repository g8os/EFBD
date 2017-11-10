package copy

import (
	"context"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/flusher"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Generator represents a tlog data generator
type Generator struct {
	sourceVdiskID string
	flusher       *flusher.Flusher
	configSource  config.Source
	jobCount      int
}

// NewGenerator creates new tlog generator
func NewGenerator(configSource config.Source, conf Config) (*Generator, error) {
	flusher, err := flusher.New(configSource, conf.FlushSize,
		conf.TargetVdiskID, conf.PrivKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create flusher")
	}
	return &Generator{
		sourceVdiskID: conf.SourceVdiskID,
		flusher:       flusher,
		configSource:  configSource,
		jobCount:      conf.JobCount,
	}, nil
}

// GenerateFromStorage generates tlog data from block storage.
// It returns last sequence flushed.
func (g *Generator) GenerateFromStorage(parentCtx context.Context) (uint64, error) {
	indices, err := storage.ListBlockIndices(g.sourceVdiskID, g.configSource)
	if err != nil {
		return 0, errors.Wrapf(err, "ListBlockIndices failed for vdisk `%v`", g.sourceVdiskID)
	}

	pool := ardb.NewPool(nil)
	defer pool.Close()

	sourceStorage, err := storage.BlockStorageFromConfig(
		g.sourceVdiskID, g.configSource, pool)
	if err != nil {
		return 0, err
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
	seq := tlog.FirstSequence

	wg.Add(1)
	go func() {
		timestamp := tlog.TimeNowTimestamp()
		defer wg.Done()

		for ic := range idxContentCh {
			select {
			case <-ctx.Done():
				return
			default:
				err = g.flusher.AddTransaction(tlog.Transaction{
					Operation: schema.OpSet,
					Sequence:  seq,
					Content:   ic.content,
					Index:     ic.idx,
					Timestamp: timestamp,
					Hash:      zerodisk.Hash(ic.content),
				})

				if err != nil {
					errCh <- err
					return
				}
				if g.flusher.Full() {
					if _, _, err := g.flusher.Flush(); err != nil {
						errCh <- err
						return
					}
				}
				if int(seq) == len(indices) {
					return
				}
				seq++
			}
		}
	}()

	go func() {
		wg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case err := <-errCh:
		return 0, err
	case <-doneCh:
		// all is good
	}

	_, _, err = g.flusher.Flush()
	log.Infof("GenerateFromStorage generates `%v` tlog data with err = %v", len(indices), err)
	return seq, err
}
