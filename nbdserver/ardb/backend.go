package ardb

import (
	"context"
	"time"

	"github.com/g8os/blockstor/log"

	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient"
	tlogserver "github.com/g8os/blockstor/tlog/tlogserver/server"
)

func newBackend(vdiskID string, blockSize int64, size uint64, storage backendStorage, storageClusterClient *storagecluster.ClusterClient, tlogClient *tlogclient.Client) (backend *Backend) {
	backend = &Backend{
		vdiskID:              vdiskID,
		blockSize:            blockSize,
		size:                 size,
		storage:              storage,
		storageClusterClient: storageClusterClient,
	}

	if tlogClient != nil {
		backend.tlogClient = tlogClient
		backend.transactionCh = make(chan transaction, transactionChCapacity)
	}

	return
}

//Backend is a nbd.Backend implementation on top of ARDB
type Backend struct {
	vdiskID              string
	blockSize            int64
	size                 uint64
	storage              backendStorage
	storageClusterClient *storagecluster.ClusterClient
	backgroundCancelFn   context.CancelFunc

	// tlog properties
	tlogClient    *tlogclient.Client
	tlogCounter   uint64
	transactionCh chan transaction
}

type transaction struct {
	Operation uint8
	Content   []byte
	Offset    uint64
	Timestamp uint64
	Size      uint64
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *Backend) WriteAt(ctx context.Context, b []byte, offset int64) (bytesWritten int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize

	length := int64(len(b))
	if offsetInsideBlock == 0 && length == ab.blockSize {
		// Option 1.
		// Which is hopefully the most common option
		// in this option we write without an offset,
		// and write a full-sized block, thus no merging required
		err = ab.storage.Set(blockIndex, b)
	} else {
		// Option 2.
		// We need to merge both contents.
		// the length can't be bigger, as that is guaranteed by gonbdserver
		_, err = ab.storage.Merge(blockIndex, offsetInsideBlock, b)
	}

	if err != nil {
		log.Debugf(
			"backend failed to WriteAt %d (offset=%d): %s",
			blockIndex, offsetInsideBlock, err.Error())
		return
	}

	// send tlog async
	if ab.tlogClient != nil {
		ab.sendTransaction(uint64(offset), schema.OpWrite, b, uint64(length))
	}

	bytesWritten = int64(len(b))
	return
}

//WriteZeroesAt implements nbd.Backend.WriteZeroesAt
func (ab *Backend) WriteZeroesAt(ctx context.Context, offset, length int64) (bytesWritten int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize

	if offsetInsideBlock == 0 && length == ab.blockSize {
		// Option 1.
		// Which is hopefully the most common option
		// in this option we have no offset, and require the full blockSize,
		// therefore we can simply delete it
		err = ab.storage.Delete(blockIndex)
	} else {
		// Option 2.
		// We need to write zeroes at an offset,
		// or the zeroes don't cover the entire block
		_, err = ab.mergeZeroes(blockIndex, offsetInsideBlock, length)
	}

	if err != nil {
		log.Debugf(
			"backend failed to WriteZeroesAt %d (offset=%d, length=%d): %s",
			blockIndex, offsetInsideBlock, length, err.Error())
		return
	}

	// send tlog async
	if ab.tlogClient != nil {
		ab.sendTransaction(uint64(offset), schema.OpWriteZeroesAt, nil, uint64(length))
	}

	bytesWritten = length
	return
}

// MergeZeroes implements storage.MergeZeroes
//  The length + offset should not exceed the blocksize
func (ab *Backend) mergeZeroes(blockIndex, offset, length int64) (mergedContent []byte, err error) {
	mergedContent, err = ab.storage.Get(blockIndex)
	if err != nil {
		return
	}

	//If the original content does not exist, no need to fill it with 0's
	if mergedContent == nil {
		return
	}
	// Assume the length of the original content == blocksize
	for i := offset; i < offset+length; i++ {
		mergedContent[i] = 0
	}

	// store new content
	err = ab.storage.Set(blockIndex, mergedContent)
	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *Backend) ReadAt(ctx context.Context, offset, length int64) (payload []byte, err error) {
	blockIndex := offset / ab.blockSize

	// try to read the payload
	payload, err = ab.storage.Get(blockIndex)
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

	return
}

//TrimAt implements nbd.Backend.TrimAt
func (ab *Backend) TrimAt(ctx context.Context, offset, length int64) (int64, error) {
	return 0, nil
}

//Flush implements nbd.Backend.Flush
func (ab *Backend) Flush(ctx context.Context) (err error) {
	err = ab.storage.Flush()
	return
}

//Close implements nbd.Backend.Close
func (ab *Backend) Close(ctx context.Context) (err error) {
	ab.storageClusterClient.Close()

	if ab.tlogClient != nil {
		err = ab.tlogClient.Close()
		if err != nil {
			return
		}
	}
	if ab.backgroundCancelFn != nil {
		ab.backgroundCancelFn()
	}

	return
}

//Geometry implements nbd.Backend.Geometry
func (ab *Backend) Geometry(ctx context.Context) (nbd.Geometry, error) {
	return nbd.Geometry{
		Size:               ab.size,
		MinimumBlockSize:   1,
		PreferredBlockSize: uint64(ab.blockSize),
		MaximumBlockSize:   32 * 1024 * 1024,
	}, nil
}

//HasFua implements nbd.Backend.HasFua
// Yes, we support fua
func (ab *Backend) HasFua(ctx context.Context) bool {
	return true
}

//HasFlush implements nbd.Backend.HasFlush
// Yes, we support flush
func (ab *Backend) HasFlush(ctx context.Context) bool {
	return true
}

// GoBackground implements Backend.GoBackground
// and the actual work is delegated to the underlying storage
func (ab *Backend) GoBackground(ctx context.Context) {
	if ab.tlogClient == nil {
		// when no tlog client is defined,
		// we do not require a background thread
		return
	}

	respChan := ab.tlogClient.Recv(transactionChCapacity)

	log.Debug(
		"starting backend background thread for vdisk:",
		ab.vdiskID)

	ctx, ab.backgroundCancelFn = context.WithCancel(ctx)
	for {
		select {
		case transaction := <-ab.transactionCh:
			err := ab.tlogClient.Send(
				transaction.Operation,
				ab.tlogCounter,
				transaction.Offset,
				transaction.Timestamp,
				transaction.Content,
				transaction.Size,
			)
			ab.tlogCounter++
			if err != nil {
				log.Infof(
					"couldn't send tlog for vdisk %s: %v",
					ab.vdiskID, err)
				continue
			}
		case tResp := <-respChan:
			if tResp.Err != nil {
				log.Infof("tlog for vdisk %s failed to recv: %v",
					ab.vdiskID, tResp.Err)
				continue
			}

			if err := tResp.Resp.Status.Error(); err != nil {
				log.Infof("tlog call for vdisk %s failed: %s",
					ab.vdiskID, err.Error())
				continue
			}
		case <-ctx.Done():
			log.Debug(
				"forcefully exit backend background thread for vdisk:",
				ab.vdiskID)
			return
		}
	}
}

func (ab *Backend) sendTransaction(offset uint64, op uint8, content []byte, length uint64) {
	ab.transactionCh <- transaction{
		Operation: op,
		Content:   content,
		Timestamp: uint64(time.Now().Unix()),
		Offset:    offset,
		Size:      length,
	}
}

var (
	transactionChCapacity = tlogserver.DefaultConfig().FlushSize * 2
)
