package block

// Reader defines reader that block on block level
type Reader interface {
	ReadBlock([]byte) ([]byte, error)
}
