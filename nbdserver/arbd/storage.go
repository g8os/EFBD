package arbd

// storage defines the interface for the actual storage implementation,
// used by ArbdBackend for a particular volume
type storage interface {
	Set(blockIndex int64, content []byte) (err error)
	Merge(blockIndex, offset int64, content []byte) (err error)
	MergeZeroes(blockIndex, offset, length int64) (err error)
	Get(blockIndex int64) (content []byte, err error)
	Delete(blockIndex int64) (err error)
	Flush() (err error)
}
