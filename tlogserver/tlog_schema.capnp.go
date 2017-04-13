package main

// AUTO GENERATED - DO NOT EDIT

import (
	capnp "zombiezen.com/go/capnproto2"
	text "zombiezen.com/go/capnproto2/encoding/text"
	schemas "zombiezen.com/go/capnproto2/schemas"
)

type TlogResponse struct{ capnp.Struct }

// TlogResponse_TypeID is the unique identifier for the type TlogResponse.
const TlogResponse_TypeID = 0x98d11ae1c78a24d9

func NewTlogResponse(s *capnp.Segment) (TlogResponse, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return TlogResponse{st}, err
}

func NewRootTlogResponse(s *capnp.Segment) (TlogResponse, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return TlogResponse{st}, err
}

func ReadRootTlogResponse(msg *capnp.Message) (TlogResponse, error) {
	root, err := msg.RootPtr()
	return TlogResponse{root.Struct()}, err
}

func (s TlogResponse) String() string {
	str, _ := text.Marshal(0x98d11ae1c78a24d9, s.Struct)
	return str
}

func (s TlogResponse) Status() int8 {
	return int8(s.Struct.Uint8(0))
}

func (s TlogResponse) SetStatus(v int8) {
	s.Struct.SetUint8(0, uint8(v))
}

func (s TlogResponse) Sequences() (capnp.UInt64List, error) {
	p, err := s.Struct.Ptr(0)
	return capnp.UInt64List{List: p.List()}, err
}

func (s TlogResponse) HasSequences() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogResponse) SetSequences(v capnp.UInt64List) error {
	return s.Struct.SetPtr(0, v.List.ToPtr())
}

// NewSequences sets the sequences field to a newly
// allocated capnp.UInt64List, preferring placement in s's segment.
func (s TlogResponse) NewSequences(n int32) (capnp.UInt64List, error) {
	l, err := capnp.NewUInt64List(s.Struct.Segment(), n)
	if err != nil {
		return capnp.UInt64List{}, err
	}
	err = s.Struct.SetPtr(0, l.List.ToPtr())
	return l, err
}

// TlogResponse_List is a list of TlogResponse.
type TlogResponse_List struct{ capnp.List }

// NewTlogResponse creates a new list of TlogResponse.
func NewTlogResponse_List(s *capnp.Segment, sz int32) (TlogResponse_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1}, sz)
	return TlogResponse_List{l}, err
}

func (s TlogResponse_List) At(i int) TlogResponse { return TlogResponse{s.List.Struct(i)} }

func (s TlogResponse_List) Set(i int, v TlogResponse) error { return s.List.SetStruct(i, v.Struct) }

// TlogResponse_Promise is a wrapper for a TlogResponse promised by a client call.
type TlogResponse_Promise struct{ *capnp.Pipeline }

func (p TlogResponse_Promise) Struct() (TlogResponse, error) {
	s, err := p.Pipeline.Struct()
	return TlogResponse{s}, err
}

type TlogBlock struct{ capnp.Struct }

// TlogBlock_TypeID is the unique identifier for the type TlogBlock.
const TlogBlock_TypeID = 0x8cf178de3c82d431

func NewTlogBlock(s *capnp.Segment) (TlogBlock, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 40, PointerCount: 1})
	return TlogBlock{st}, err
}

func NewRootTlogBlock(s *capnp.Segment) (TlogBlock, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 40, PointerCount: 1})
	return TlogBlock{st}, err
}

func ReadRootTlogBlock(msg *capnp.Message) (TlogBlock, error) {
	root, err := msg.RootPtr()
	return TlogBlock{root.Struct()}, err
}

func (s TlogBlock) String() string {
	str, _ := text.Marshal(0x8cf178de3c82d431, s.Struct)
	return str
}

func (s TlogBlock) VolumeId() uint32 {
	return s.Struct.Uint32(0)
}

func (s TlogBlock) SetVolumeId(v uint32) {
	s.Struct.SetUint32(0, v)
}

func (s TlogBlock) Sequence() uint64 {
	return s.Struct.Uint64(8)
}

func (s TlogBlock) SetSequence(v uint64) {
	s.Struct.SetUint64(8, v)
}

func (s TlogBlock) Lba() uint64 {
	return s.Struct.Uint64(16)
}

func (s TlogBlock) SetLba(v uint64) {
	s.Struct.SetUint64(16, v)
}

func (s TlogBlock) Size() uint32 {
	return s.Struct.Uint32(4)
}

func (s TlogBlock) SetSize(v uint32) {
	s.Struct.SetUint32(4, v)
}

func (s TlogBlock) Crc32() uint32 {
	return s.Struct.Uint32(24)
}

func (s TlogBlock) SetCrc32(v uint32) {
	s.Struct.SetUint32(24, v)
}

func (s TlogBlock) Data() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return []byte(p.Data()), err
}

func (s TlogBlock) HasData() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogBlock) SetData(v []byte) error {
	return s.Struct.SetData(0, v)
}

func (s TlogBlock) Timestamp() uint64 {
	return s.Struct.Uint64(32)
}

func (s TlogBlock) SetTimestamp(v uint64) {
	s.Struct.SetUint64(32, v)
}

// TlogBlock_List is a list of TlogBlock.
type TlogBlock_List struct{ capnp.List }

// NewTlogBlock creates a new list of TlogBlock.
func NewTlogBlock_List(s *capnp.Segment, sz int32) (TlogBlock_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 40, PointerCount: 1}, sz)
	return TlogBlock_List{l}, err
}

func (s TlogBlock_List) At(i int) TlogBlock { return TlogBlock{s.List.Struct(i)} }

func (s TlogBlock_List) Set(i int, v TlogBlock) error { return s.List.SetStruct(i, v.Struct) }

// TlogBlock_Promise is a wrapper for a TlogBlock promised by a client call.
type TlogBlock_Promise struct{ *capnp.Pipeline }

func (p TlogBlock_Promise) Struct() (TlogBlock, error) {
	s, err := p.Pipeline.Struct()
	return TlogBlock{s}, err
}

type TlogAggregation struct{ capnp.Struct }

// TlogAggregation_TypeID is the unique identifier for the type TlogAggregation.
const TlogAggregation_TypeID = 0xe46ab5b4b619e094

func NewTlogAggregation(s *capnp.Segment) (TlogAggregation, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 24, PointerCount: 3})
	return TlogAggregation{st}, err
}

func NewRootTlogAggregation(s *capnp.Segment) (TlogAggregation, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 24, PointerCount: 3})
	return TlogAggregation{st}, err
}

func ReadRootTlogAggregation(msg *capnp.Message) (TlogAggregation, error) {
	root, err := msg.RootPtr()
	return TlogAggregation{root.Struct()}, err
}

func (s TlogAggregation) String() string {
	str, _ := text.Marshal(0xe46ab5b4b619e094, s.Struct)
	return str
}

func (s TlogAggregation) Name() (string, error) {
	p, err := s.Struct.Ptr(0)
	return p.Text(), err
}

func (s TlogAggregation) HasName() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) NameBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return p.TextBytes(), err
}

func (s TlogAggregation) SetName(v string) error {
	return s.Struct.SetText(0, v)
}

func (s TlogAggregation) Size() uint64 {
	return s.Struct.Uint64(0)
}

func (s TlogAggregation) SetSize(v uint64) {
	s.Struct.SetUint64(0, v)
}

func (s TlogAggregation) Timestamp() uint64 {
	return s.Struct.Uint64(8)
}

func (s TlogAggregation) SetTimestamp(v uint64) {
	s.Struct.SetUint64(8, v)
}

func (s TlogAggregation) VolumeId() uint32 {
	return s.Struct.Uint32(16)
}

func (s TlogAggregation) SetVolumeId(v uint32) {
	s.Struct.SetUint32(16, v)
}

func (s TlogAggregation) Blocks() (TlogBlock_List, error) {
	p, err := s.Struct.Ptr(1)
	return TlogBlock_List{List: p.List()}, err
}

func (s TlogAggregation) HasBlocks() bool {
	p, err := s.Struct.Ptr(1)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetBlocks(v TlogBlock_List) error {
	return s.Struct.SetPtr(1, v.List.ToPtr())
}

// NewBlocks sets the blocks field to a newly
// allocated TlogBlock_List, preferring placement in s's segment.
func (s TlogAggregation) NewBlocks(n int32) (TlogBlock_List, error) {
	l, err := NewTlogBlock_List(s.Struct.Segment(), n)
	if err != nil {
		return TlogBlock_List{}, err
	}
	err = s.Struct.SetPtr(1, l.List.ToPtr())
	return l, err
}

func (s TlogAggregation) Prev() ([]byte, error) {
	p, err := s.Struct.Ptr(2)
	return []byte(p.Data()), err
}

func (s TlogAggregation) HasPrev() bool {
	p, err := s.Struct.Ptr(2)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetPrev(v []byte) error {
	return s.Struct.SetData(2, v)
}

// TlogAggregation_List is a list of TlogAggregation.
type TlogAggregation_List struct{ capnp.List }

// NewTlogAggregation creates a new list of TlogAggregation.
func NewTlogAggregation_List(s *capnp.Segment, sz int32) (TlogAggregation_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 24, PointerCount: 3}, sz)
	return TlogAggregation_List{l}, err
}

func (s TlogAggregation_List) At(i int) TlogAggregation { return TlogAggregation{s.List.Struct(i)} }

func (s TlogAggregation_List) Set(i int, v TlogAggregation) error {
	return s.List.SetStruct(i, v.Struct)
}

// TlogAggregation_Promise is a wrapper for a TlogAggregation promised by a client call.
type TlogAggregation_Promise struct{ *capnp.Pipeline }

func (p TlogAggregation_Promise) Struct() (TlogAggregation, error) {
	s, err := p.Pipeline.Struct()
	return TlogAggregation{s}, err
}

const schema_f4533cbae6e08506 = "x\xdat\x93OH\x14a\x18\xc6\x9f\xe7\xfdvV\x05" +
	"u\x1bw\x0e\x15F\x10vP2\xfcs\x13\xc3\xecT" +
	"\x9d\xfc4\xe8\x18\xe3\xf8\xb1\x99\xb3;\x9b3Zt\x0c" +
	"\xbaD\xd0\xc5\x83\x1d\x02\x83\x0e\x05^\x02\x0d\x8a\x02\x83" +
	"$\x03\x83\x04\x83\xba\x84Et\x0f:u\x99\xf8\xdcf" +
	"\xdd\xc2n\xef<<3\xcf\xfb>?\xa6o\x82\xa7\xa5" +
	"\xdf\x89\x04\xd0\x9dN>\xed\xdf\xbe9\xfc\xf9\xfa\x8f;" +
	"\xd0\x1dt\xd2\xfc\xad\x9d\xef\xcf\x87'~\xc2a\x13P" +
	"<\xc2\xaf\xc5n;\x0d\x1e\xe7]\x82\xe9\xa7\xae\xdbo" +
	"\xbe\x1c\xdeZ\xb4n\xfe\xeb~&\xbf\x8a\xebb\xa7W" +
	"r\x0dL\x17v\x0e=]Y\xbd\xf2\xcd\x9aU\x83Y" +
	"\xd9\x0f\xf6\xab\x0e\x16Gw\xc7S\xea\"\xd1\x9b&a" +
	"T\xba\x14\x07\x97\xc5\x94\xfd\x93\x81_\xadT\x87.\x84" +
	"Q\xe9L\xd8\x14\x053c\xa4\xeeT9 G\xc0]" +
	"=\x0f\xe8\x15E\xbd&tI\x8fV|i\xc5\x17\x8a" +
	"zC\xe8\x8ax\x14\xc0]?\x06\xe85E\xbd)t" +
	"\x15=*\xc0}\xdb\x03\xe8\xd7\x8a\xfa\xbd\xd0\xcd\xe5=" +
	"\xe6\x00\xf7\xdd\x00\xa07\x14\xf5\xb6\x90\x8eG\x07p\xb7" +
	"\xacqSQ\x7f\x14\xba\xf9\x9c\xc7<\xe0~\x18\x07\xf4" +
	"\xb6\xa2\xde\x11\xa6\xf3Q8W6\xe7\xa6\x00\xb0\x19\xc2" +
	"f0\x8d\xcd\xd59S\x09\x8c\xd5Z l\x01\x9b\xc2" +
	"I?\x9b\x0b\xf1\xf4\x0d\x93\x99\x8f\x06\xb3\xc1\xe0@\xf6" +
	"T\x98\xf2\x13\x9fm\x10\xb6\x81i2]6q\xe2\x97" +
	"\xc1j\xf6\xee\xffJ\x1a7#q5\xaa\xc4\xc6\xf6\xd4" +
	"\\\xef\xa9{\x08\xd0]\x8a\xbaO\x98\xd5\xd4k\xd7?" +
	"\xa1\xa8\xcf\x0aG\xe2\xc4O\xe6b\x0a\x84\xd2\xb89c" +
	"\xb6\x83c\x8a\xbb\xc1\xed\x0d\xc1\xea\xef\xe0\xd1Ri\xd6" +
	"\x94\xfcd:b\xc5f\x1f\xacg\xdf\xb3\xdd-(\xea" +
	"\xa5\xbd\xec\xfbV[T\xd4\x0f-\"\xd6\x10=\xb0\x0b" +
	"-)\xeae\x8b(WC\xf4\xd8\xc2|\xa4\xa8W," +
	"\"\xd6\x10=\xb1\xe7,\xff!\xecH\x8d\xd1z\xcf\x1e" +
	"\xe1B\xc5/\x1b\xb6B\xd8\x9a\xf5\\/n\x9f2\xf7" +
	"\x8172\x19F\xc1L\xfd\xfa\x03{\xff\x07h\xc5B" +
	"u\xd6\xccg\x84~\x07\x00\x00\xff\xff2\x09\xb8\x86"

func init() {
	schemas.Register(schema_f4533cbae6e08506,
		0x8cf178de3c82d431,
		0x98d11ae1c78a24d9,
		0xe46ab5b4b619e094)
}
