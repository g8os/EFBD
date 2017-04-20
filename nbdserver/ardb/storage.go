package ardb

// backendStorage defines the interface for the actual storage implementation,
// used by ArbdBackend for a particular vdisk
type backendStorage interface {
	Set(blockIndex int64, content []byte) (err error)
	Merge(blockIndex, offset int64, content []byte) (err error)
	Get(blockIndex int64) (content []byte, err error)
	Delete(blockIndex int64) (err error)
	Flush() (err error)
}
