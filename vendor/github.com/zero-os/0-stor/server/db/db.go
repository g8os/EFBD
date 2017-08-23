package db

// DB interface is the interface defining how to interact with the key value store
type DB interface {
	Get(key []byte) ([]byte, error)
	Exists(key []byte) (bool, error)
	Filter(prefix []byte, start int, count int) ([][]byte, error)
	List(prefix []byte) ([][]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Close() error
}
