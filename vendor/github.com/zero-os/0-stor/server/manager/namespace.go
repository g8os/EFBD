package manager

import (
	"fmt"

	"github.com/zero-os/0-stor/server/db"
)

type NamespaceManager struct {
	db db.DB
}

func NewNamespaceManager(db db.DB) *NamespaceManager {
	return &NamespaceManager{
		db: db,
	}
}

func nsKey(label string) []byte {
	return []byte(fmt.Sprintf("%s:%s", NAMESPACE_PREFIX, label))
}

func (mgr *NamespaceManager) Get(label string) (*db.Namespace, error) {
	b, err := mgr.db.Get(nsKey(label))
	if err != nil {
		return nil, err
	}

	ns := db.NewNamespace()
	err = ns.Decode(b)
	return ns, err
}

// Count return the number of object present in a namespace
func (mgr *NamespaceManager) Count(label string) (int, error) {
	keys, err := mgr.db.List([]byte(label))
	if err != nil {
		return 0, err
	}

	return len(keys), nil
}

// Create namespace if doesn't exists
func (mgr *NamespaceManager) Create(label string) error {
	exists, err := mgr.db.Exists([]byte(label))

	if err != nil {
		return err
	}

	if exists{
		return nil
	}

	bytes, err := db.NewNamespace().Encode()
	if err == nil{
		err = mgr.db.Set([]byte(label), bytes)
	}
	return err
}

