package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
)

const (
	POLYNOMIAL  = 0xD5828281
	RefIDCount  = 160
	RefIDLenght = 16
)

var (
	ErrReferenceTooLong    = fmt.Errorf("too long reference ID, max = %v", RefIDLenght)
	ErrReferenceListTooBig = fmt.Errorf("too much reference ID, max = %v", RefIDCount)
)

var (
	tabPolynomial *crc32.Table
	nilRefList    [RefIDLenght]byte
)

func init() {
	tabPolynomial = crc32.MakeTable(POLYNOMIAL)
}

// Object is the data structure used to encode, decode object on the disk
type Object struct {
	// A fixed size slice of reference list
	// If not full, all zero reference is used as the sentinel of valid reference list.
	// All reference after that value should be ignored
	ReferenceList [RefIDCount][RefIDLenght]byte
	CRC           uint32
	Data          []byte
}

func NewObject(data []byte) *Object {
	return &Object{
		Data: data,
	}
}

func (o *Object) Encode() ([]byte, error) {
	o.CRC = crc32.Checksum([]byte(o.Data), tabPolynomial)

	var err error
	buf := &bytes.Buffer{}

	for i := range o.ReferenceList {
		err = binary.Write(buf, binary.LittleEndian, o.ReferenceList[i][:])
		if err != nil {
			return nil, err
		}
	}

	err = binary.Write(buf, binary.LittleEndian, o.CRC)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, o.Data)

	return buf.Bytes(), err
}

func (o *Object) Decode(b []byte) error {
	var err error
	r := bytes.NewReader(b)

	refBuf := make([]byte, 16)
	for i := range o.ReferenceList {
		err = binary.Read(r, binary.LittleEndian, refBuf)
		if err != nil {
			return err
		}
		n := copy(o.ReferenceList[i][:], refBuf)
		if n != 16 {
			return fmt.Errorf("error decoding reference list")
		}
	}

	err = binary.Read(r, binary.LittleEndian, &o.CRC)
	if err != nil {
		return err
	}

	// read the rest of the data from the read
	o.Data, err = ioutil.ReadAll(r)
	return err
}

func (o *Object) SetReferenceList(refList []string) error {
	if len(refList) > RefIDCount {
		return ErrReferenceListTooBig
	}

	// copy
	for i := range refList {
		if len(refList[i]) > RefIDLenght {
			return ErrReferenceTooLong
		}
		copy(o.ReferenceList[i][:], []byte(refList[i]))
	}

	// create sentinel
	if len(refList) < RefIDCount {
		copy(o.ReferenceList[len(refList)][:], nilRefList[:])
	}

	return nil
}

// AppendReferenceList appends given reference list to the object.
// The append operation doesn't check whether the existing reference
// already exist
func (o *Object) AppendReferenceList(refList []string) error {
	numRefList := o.countRefList()

	// make sure the append operation doesn't make our
	// ref list exceed the max len
	if len(refList)+numRefList > RefIDCount {
		return ErrReferenceListTooBig
	}

	// append it
	for i, ref := range refList {
		copy(o.ReferenceList[i+numRefList][:], []byte(ref))
	}

	currNumRefList := numRefList + len(refList)
	if currNumRefList < RefIDCount {
		copy(o.ReferenceList[currNumRefList][:], nilRefList[:])
	}
	return nil
}

// RemoveReferenceList removes given reference list from the object.
func (o *Object) RemoveReferenceList(refList []string) error {
	// creates map of existing reference list for easier search
	// it might good for big refList but seems bad
	// for the little one
	refMap := make(map[string]struct{}, len(refList))
	for _, ref := range refList {
		refMap[ref] = struct{}{}
	}

	newRefList := make([]string, 0, RefIDCount)
	for _, ref := range o.ReferenceList {
		strRef := referenceToStr(ref)
		if _, exists := refMap[strRef]; exists {
			continue
		}
		newRefList = append(newRefList, strRef)
	}
	return o.SetReferenceList(newRefList)
}

// GetReferenceListStr returns referece list in the form of []string
func (o *Object) GetReferenceListStr() []string {
	refListStr := make([]string, 0, len(o.ReferenceList))
	for _, ref := range o.ReferenceList {
		if isNilReference(ref) {
			return refListStr
		}
		refListStr = append(refListStr, referenceToStr(ref))
	}

	return refListStr
}

func referenceToStr(ref [RefIDLenght]byte) string {
	return string(bytes.TrimRight(ref[:], "\x00"))
}

// count number of reference list in this object
func (o *Object) countRefList() int {
	for i, ref := range o.ReferenceList {
		if isNilReference(ref) {
			return i
		}
	}
	return len(o.ReferenceList)
}

func isNilReference(ref [RefIDLenght]byte) bool {
	return bytes.Compare(ref[:], nilRefList[:]) == 0
}

// ValidCRC compare the content of the data and the crc, return true if CRC match data, false otherwrise
func (o *Object) ValidCRC() bool {
	return crc32.Checksum(o.Data, tabPolynomial) == o.CRC
}
