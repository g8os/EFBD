package arbd

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/g8os/gonbdserver/nbd"
)

//Backend is a nbd.Backend implementation on top of ARDB
type Backend struct {
	blockSize int64
	size      uint64
	storage   storage
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *Backend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize

	length := int64(len(b))
	if length != ab.blockSize {
		err = fmt.Errorf(
			"expected a block of %d bytes, but received %d bytes instead",
			ab.blockSize, length)
	}

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
		err = ab.storage.Merge(blockIndex, offsetInsideBlock, b)
	}

	if err != nil {
		return
	}

	if fua {
		err = ab.Flush(ctx)
		if err != nil {
			return
		}
	}

	bytesWritten = int64(len(b))
	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *Backend) ReadAt(ctx context.Context, b []byte, offset int64) (bytesRead int64, err error) {
	blockIndex := offset / ab.blockSize
	offsetInsideBlock := offset % ab.blockSize
	contentLength := int64(len(b))

	content, err := ab.storage.Get(blockIndex)
	if err != nil {
		return
	}
	if len(content) == 0 {
		bytesRead = contentLength
		return
	}

	copy(b, content[offsetInsideBlock:])
	bytesRead = contentLength
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
