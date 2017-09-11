package main

import (
	"context"
	"fmt"
	"time"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/nbd/nbdserver/statistics"
)

func newBackend(vdiskID string, size uint64, blockSize int64, storage storage.BlockStorage, vComp *vdiskCompletion, connProvider ardb.ConnProvider, vdiskStatsLogger statistics.VdiskLogger) *backend {
	return &backend{
		vdiskID:          vdiskID,
		blockSize:        blockSize,
		size:             size,
		storage:          storage,
		connProvider:     connProvider,
		vComp:            vComp,
		vdiskStatsLogger: vdiskStatsLogger,
	}
}

// backend is a nbd.Backend implementation on top of ARDB
type backend struct {
	vdiskID          string
	blockSize        int64
	size             uint64
	storage          storage.BlockStorage
	connProvider     ardb.ConnProvider
	vComp            *vdiskCompletion
	vdiskStatsLogger statistics.VdiskLogger
}

// WriteAt implements nbd.Backend.WriteAt
func (ab *backend) WriteAt(ctx context.Context, b []byte, offset int64) (bytesWritten int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize

	length := int64(len(b))
	if offsetInsideBlock == 0 && length == ab.blockSize {
		// Option 1.
		// Which is hopefully the most common option
		// in this option we write without an offset,
		// and write a full-sized block, thus no merging required
		err = ab.storage.SetBlock(blockIndex, b)
	} else {
		// Option 2.
		// We need to merge both contents.
		// the length can't be bigger, as that is guaranteed by gonbdserver
		err = ab.merge(blockIndex, offsetInsideBlock, b)
	}

	if err != nil {
		log.Debugf(
			"backend failed to WriteAt %d (offset=%d): %s",
			blockIndex, offsetInsideBlock, err.Error())
		return
	}

	bytesWritten = int64(len(b))
	ab.vdiskStatsLogger.LogWriteOperation(bytesWritten)
	return
}

// WriteZeroesAt implements nbd.Backend.WriteZeroesAt
func (ab *backend) WriteZeroesAt(ctx context.Context, offset, length int64) (bytesWritten int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize

	if offsetInsideBlock == 0 && length == ab.blockSize {
		// Option 1.
		// Which is hopefully the most common option
		// in this option we have no offset, and require the full blockSize,
		// therefore we can simply delete it
		err = ab.storage.DeleteBlock(blockIndex)
	} else {
		// Option 2.
		// We need to write zeroes at an offset,
		// or the zeroes don't cover the entire block
		err = ab.mergeZeroes(blockIndex, offsetInsideBlock, length)
	}

	if err != nil {
		log.Debugf(
			"backend failed to WriteZeroesAt %d (offset=%d, length=%d): %s",
			blockIndex, offsetInsideBlock, length, err.Error())
		return
	}

	bytesWritten = length
	ab.vdiskStatsLogger.LogWriteOperation(bytesWritten)
	return
}

// mergeZeroes merges zeroes into an existing block,
// or does nothing in case the block does not exist yet.
// The length + offset should not exceed the blocksize.
func (ab *backend) mergeZeroes(blockIndex, offset, length int64) error {
	mergedContent, err := ab.storage.GetBlock(blockIndex)
	if err != nil {
		return err
	}

	//If the original content does not exist, no need to fill it with 0's
	if mergedContent == nil {
		return nil
	}
	// Assume the length of the original content == blocksize
	for i := offset; i < offset+length; i++ {
		mergedContent[i] = 0
	}

	// store new content
	return ab.storage.SetBlock(blockIndex, mergedContent)
}

// merge a block into an existing block....
func (ab *backend) merge(blockIndex, offset int64, content []byte) error {
	mergedContent, _ := ab.storage.GetBlock(blockIndex)

	// create old content from scratch or expand it to the blocksize,
	// in case no old content was defined or it was defined but too small
	if ocl := int64(len(mergedContent)); ocl == 0 {
		mergedContent = make([]byte, ab.blockSize)
	} else if ocl < ab.blockSize {
		oc := make([]byte, ab.blockSize)
		copy(oc, mergedContent)
		mergedContent = oc
	}

	// copy in new content
	copy(mergedContent[offset:], content)

	// store new content
	return ab.storage.SetBlock(blockIndex, mergedContent)
}

// ReadAt implements nbd.Backend.ReadAt
func (ab *backend) ReadAt(ctx context.Context, offset, length int64) (payload []byte, err error) {
	blockIndex := offset / ab.blockSize

	// try to read the payload
	payload, err = ab.storage.GetBlock(blockIndex)
	if err != nil {
		return
	}

	// calculate the local offset and the length of the read payload
	offsetInsideBlock := offset % ab.blockSize
	contentLength := int64(len(payload))

	if totalLength := offsetInsideBlock + length; contentLength >= totalLength {
		// Option 1
		// we have read content,
		// and the content is long enough to cover everything including the offset
		payload = payload[offsetInsideBlock:totalLength]
	} else {
		// Option 2
		// We have read no content,
		// or we have read content, but it might be shorter than the local offset,
		// or the read content might only cover partly beyond the local offset.
		p := make([]byte, length)
		if contentLength >= offsetInsideBlock {
			copy(p, payload[offsetInsideBlock:])
		}
		payload = p
	}

	ab.vdiskStatsLogger.LogReadOperation(int64(len(payload)))
	return
}

// TrimAt implements nbd.Backend.TrimAt
func (ab *backend) TrimAt(ctx context.Context, offset, length int64) (int64, error) {
	return 0, nil
}

// Flush implements nbd.Backend.Flush
func (ab *backend) Flush(ctx context.Context) (err error) {
	err = ab.storage.Flush()
	return
}

// Close implements nbd.Backend.Close
func (ab *backend) Close(ctx context.Context) (err error) {
	ab.vdiskStatsLogger.Close()

	if ab.connProvider != nil {
		ab.connProvider.Close()
	}

	err = ab.storage.Close()
	return
}

// Geometry implements nbd.Backend.Geometry
func (ab *backend) Geometry(ctx context.Context) (nbd.Geometry, error) {
	return nbd.Geometry{
		Size:               ab.size,
		MinimumBlockSize:   1,
		PreferredBlockSize: uint64(ab.blockSize),
		MaximumBlockSize:   32 * 1024 * 1024,
	}, nil
}

// HasFua implements nbd.Backend.HasFua
// Yes, we support fua
func (ab *backend) HasFua(ctx context.Context) bool {
	return true
}

// HasFlush implements nbd.Backend.HasFlush
// Yes, we support flush
func (ab *backend) HasFlush(ctx context.Context) bool {
	return true
}

// GoBackground implements Backend.GoBackground
// ensuring that a backend gracefully exists when a SIGTERM signal is received.
func (ab *backend) GoBackground(ctx context.Context) {
	log.Debugf("starting background thread for vdisk %s's backend", ab.vdiskID)

	ab.vComp.Add()
	defer ab.vComp.Done()

	// wait until some event frees up this goroutine,
	// either because the context is Done,
	// or because we received a SIGTERM handler,
	// whatever comes first.
	select {
	case <-ctx.Done():
		log.Debugf("aborting background thread for vdisk %s's backend", ab.vdiskID)

	case <-ab.vComp.Stopped():
		log.Infof("vdisk '%s' received `Stop` command from vdisk completion", ab.vdiskID)

		// execute flush
		done := make(chan error, 1)
		go func() {
			done <- ab.storage.Flush()
		}()

		var err error

		// wait for Flush completion or timed out
		select {
		case err = <-done:
			log.Infof("vdisk '%s' finished the flush under SIGTERM handler", ab.vdiskID)

		case <-time.After(flushWaitRetry * flushWaitRetryNum):
			// TODO :
			// - how long is the reasonable waiting time?
			// - put this value in the config?
			err = fmt.Errorf("vdisk '%s' SIGTERM flush timed out", ab.vdiskID)
		}

		// did flushing fail?
		if err != nil {
			ab.vComp.AddError(err)
		}

		log.Debugf("exit from SIGTERM handler for vdisk %s", ab.vdiskID)
	}
}
