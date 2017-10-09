package tlog

// Transaction defines a tlog transaction
type Transaction struct {
	Operation uint8
	Sequence  uint64
	Content   []byte
	Hash      []byte
	Index     int64
	Timestamp int64
}
