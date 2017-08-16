package storserver

import "github.com/zero-os/0-stor/server/db"

// Server defines a 0-stor server
type StoreServer interface {
	Listen(string) (string, error)
	Close()
	DB() db.DB
	Addr() string
}
