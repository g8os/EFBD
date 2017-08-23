package db

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
)

// object is the data structure used to encode, decode object on the disk
type Namespace struct {
	// Space reserved in this namespace, in bytes
	Reserved uint64
	Label    string
	// NrObjects         int64 // Should it be computed at runtime ?
	// RequestPerHour    int64
	// NrRequests        int64
}

func NewNamespace() *Namespace {
	return &Namespace{}
}

func (n *Namespace) Encode() ([]byte, error) {
	var err error
	buf := &bytes.Buffer{}

	err = binary.Write(buf, binary.LittleEndian, n.Reserved)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, []byte(n.Label))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (n *Namespace) Decode(b []byte) error {
	if n == nil {
		n = &Namespace{}
	}

	var err error
	r := bytes.NewReader(b)
	err = binary.Read(r, binary.LittleEndian, &n.Reserved)
	if err != nil {
		return err
	}

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	n.Label = string(buf)

	return nil
}
