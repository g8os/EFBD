package manager

import (
	"fmt"

	"github.com/zero-os/0-stor/server/db"
)

type ObjectManager struct {
	db        db.DB
	namespace string
}

const (
	_ = iota
	RefListOpSet
	RefListOpAppend
	RefListOpRemove
)

func NewObjectManager(namespace string, db db.DB) *ObjectManager {
	return &ObjectManager{
		namespace: namespace,
		db:        db,
	}
}

func objKey(namespace string, key []byte) []byte {
	return []byte(fmt.Sprintf("%s:%s", namespace, key))
}

// Set saved an object into the key-value store as a blob of bytes
func (mgr *ObjectManager) Set(key []byte, data []byte, referenceList []string) error {
	obj := db.NewObject(data)
	for i := range referenceList {
		copy(obj.ReferenceList[i][:], []byte(referenceList[i]))
	}

	b, err := obj.Encode()
	if err != nil {
		return err
	}

	k := objKey(mgr.namespace, key)
	return mgr.db.Set(k, b)
}

func (mgr *ObjectManager) List(start, count int) ([][]byte, error) {
	prefix := fmt.Sprintf("%s:", mgr.namespace)
	keys, err := mgr.db.List([]byte(prefix))
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		fmt.Println(string(key))
	}
	// remove namespace prefix
	for i := range keys {
		keys[i] = keys[i][len(mgr.namespace)+1:]
	}
	return keys, nil
}

func (mgr *ObjectManager) Get(key []byte) (*db.Object, error) {
	b, err := mgr.db.Get(objKey(mgr.namespace, key))
	if err != nil {
		return nil, err
	}

	obj := db.NewObject(nil)
	err = obj.Decode(b)
	return obj, err
}

func (mgr *ObjectManager) Delete(key []byte) error {
	return mgr.db.Delete(objKey(mgr.namespace, key))
}

func (mgr *ObjectManager) Exists(key []byte) (bool, error) {
	return mgr.db.Exists(objKey(mgr.namespace, key))
}

func (mgr *ObjectManager) UpdateReferenceList(key []byte, refList []string, op int) error {
	b, err := mgr.db.Get(objKey(mgr.namespace, key))
	if err != nil {
		return err
	}

	obj := db.NewObject(nil)
	if err = obj.Decode(b); err != nil {
		return err
	}

	switch op {
	case RefListOpSet:
		err = obj.SetReferenceList(refList)
	case RefListOpRemove:
		err = obj.RemoveReferenceList(refList)
	case RefListOpAppend:
		err = obj.AppendReferenceList(refList)
	default:
		err = fmt.Errorf("invalid reference list operation")
	}
	if err != nil {
		return err
	}

	b, err = obj.Encode()
	if err != nil {
		return err
	}

	return mgr.db.Set(objKey(mgr.namespace, key), b)
}

type CheckStatus string

var (
	CheckStatusOK        CheckStatus = "ok"
	CheckStatusCorrupted CheckStatus = "corrupted"
	CheckStatusMissing   CheckStatus = "missing"
)

func (mgr *ObjectManager) Check(key []byte) (CheckStatus, error) {
	b, err := mgr.db.Get(objKey(mgr.namespace, key))
	if err != nil {
		if err == db.ErrNotFound {
			return CheckStatusMissing, nil
		}
		return "", err
	}

	obj := db.Object{}
	if err := obj.Decode(b); err != nil {
		return "", err
	}

	if obj.ValidCRC() {
		return CheckStatusOK, nil
	}
	return CheckStatusCorrupted, nil
}
