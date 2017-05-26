// Code generated by capnpc-go. DO NOT EDIT.

package schema

import (
	capnp "zombiezen.com/go/capnproto2"
	text "zombiezen.com/go/capnproto2/encoding/text"
	schemas "zombiezen.com/go/capnproto2/schemas"
)

type HandshakeRequest struct{ capnp.Struct }

// HandshakeRequest_TypeID is the unique identifier for the type HandshakeRequest.
const HandshakeRequest_TypeID = 0xe0d4e6d68fa24ac0

func NewHandshakeRequest(s *capnp.Segment) (HandshakeRequest, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1})
	return HandshakeRequest{st}, err
}

func NewRootHandshakeRequest(s *capnp.Segment) (HandshakeRequest, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1})
	return HandshakeRequest{st}, err
}

func ReadRootHandshakeRequest(msg *capnp.Message) (HandshakeRequest, error) {
	root, err := msg.RootPtr()
	return HandshakeRequest{root.Struct()}, err
}

func (s HandshakeRequest) String() string {
	str, _ := text.Marshal(0xe0d4e6d68fa24ac0, s.Struct)
	return str
}

func (s HandshakeRequest) Version() uint32 {
	return s.Struct.Uint32(0)
}

func (s HandshakeRequest) SetVersion(v uint32) {
	s.Struct.SetUint32(0, v)
}

func (s HandshakeRequest) VdiskID() (string, error) {
	p, err := s.Struct.Ptr(0)
	return p.Text(), err
}

func (s HandshakeRequest) HasVdiskID() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s HandshakeRequest) VdiskIDBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return p.TextBytes(), err
}

func (s HandshakeRequest) SetVdiskID(v string) error {
	return s.Struct.SetText(0, v)
}

func (s HandshakeRequest) FirstSequence() uint64 {
	return s.Struct.Uint64(8)
}

func (s HandshakeRequest) SetFirstSequence(v uint64) {
	s.Struct.SetUint64(8, v)
}

// HandshakeRequest_List is a list of HandshakeRequest.
type HandshakeRequest_List struct{ capnp.List }

// NewHandshakeRequest creates a new list of HandshakeRequest.
func NewHandshakeRequest_List(s *capnp.Segment, sz int32) (HandshakeRequest_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1}, sz)
	return HandshakeRequest_List{l}, err
}

func (s HandshakeRequest_List) At(i int) HandshakeRequest { return HandshakeRequest{s.List.Struct(i)} }

func (s HandshakeRequest_List) Set(i int, v HandshakeRequest) error {
	return s.List.SetStruct(i, v.Struct)
}

// HandshakeRequest_Promise is a wrapper for a HandshakeRequest promised by a client call.
type HandshakeRequest_Promise struct{ *capnp.Pipeline }

func (p HandshakeRequest_Promise) Struct() (HandshakeRequest, error) {
	s, err := p.Pipeline.Struct()
	return HandshakeRequest{s}, err
}

type HandshakeResponse struct{ capnp.Struct }

// HandshakeResponse_TypeID is the unique identifier for the type HandshakeResponse.
const HandshakeResponse_TypeID = 0xee959a7d96c96641

func NewHandshakeResponse(s *capnp.Segment) (HandshakeResponse, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return HandshakeResponse{st}, err
}

func NewRootHandshakeResponse(s *capnp.Segment) (HandshakeResponse, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return HandshakeResponse{st}, err
}

func ReadRootHandshakeResponse(msg *capnp.Message) (HandshakeResponse, error) {
	root, err := msg.RootPtr()
	return HandshakeResponse{root.Struct()}, err
}

func (s HandshakeResponse) String() string {
	str, _ := text.Marshal(0xee959a7d96c96641, s.Struct)
	return str
}

func (s HandshakeResponse) Version() uint32 {
	return s.Struct.Uint32(0)
}

func (s HandshakeResponse) SetVersion(v uint32) {
	s.Struct.SetUint32(0, v)
}

func (s HandshakeResponse) Status() int8 {
	return int8(s.Struct.Uint8(4))
}

func (s HandshakeResponse) SetStatus(v int8) {
	s.Struct.SetUint8(4, uint8(v))
}

// HandshakeResponse_List is a list of HandshakeResponse.
type HandshakeResponse_List struct{ capnp.List }

// NewHandshakeResponse creates a new list of HandshakeResponse.
func NewHandshakeResponse_List(s *capnp.Segment, sz int32) (HandshakeResponse_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0}, sz)
	return HandshakeResponse_List{l}, err
}

func (s HandshakeResponse_List) At(i int) HandshakeResponse {
	return HandshakeResponse{s.List.Struct(i)}
}

func (s HandshakeResponse_List) Set(i int, v HandshakeResponse) error {
	return s.List.SetStruct(i, v.Struct)
}

// HandshakeResponse_Promise is a wrapper for a HandshakeResponse promised by a client call.
type HandshakeResponse_Promise struct{ *capnp.Pipeline }

func (p HandshakeResponse_Promise) Struct() (HandshakeResponse, error) {
	s, err := p.Pipeline.Struct()
	return HandshakeResponse{s}, err
}

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
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 40, PointerCount: 2})
	return TlogBlock{st}, err
}

func NewRootTlogBlock(s *capnp.Segment) (TlogBlock, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 40, PointerCount: 2})
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

func (s TlogBlock) Sequence() uint64 {
	return s.Struct.Uint64(0)
}

func (s TlogBlock) SetSequence(v uint64) {
	s.Struct.SetUint64(0, v)
}

func (s TlogBlock) Offset() uint64 {
	return s.Struct.Uint64(8)
}

func (s TlogBlock) SetOffset(v uint64) {
	s.Struct.SetUint64(8, v)
}

func (s TlogBlock) Size() uint64 {
	return s.Struct.Uint64(16)
}

func (s TlogBlock) SetSize(v uint64) {
	s.Struct.SetUint64(16, v)
}

func (s TlogBlock) Hash() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return []byte(p.Data()), err
}

func (s TlogBlock) HasHash() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogBlock) SetHash(v []byte) error {
	return s.Struct.SetData(0, v)
}

func (s TlogBlock) Data() ([]byte, error) {
	p, err := s.Struct.Ptr(1)
	return []byte(p.Data()), err
}

func (s TlogBlock) HasData() bool {
	p, err := s.Struct.Ptr(1)
	return p.IsValid() || err != nil
}

func (s TlogBlock) SetData(v []byte) error {
	return s.Struct.SetData(1, v)
}

func (s TlogBlock) Timestamp() uint64 {
	return s.Struct.Uint64(24)
}

func (s TlogBlock) SetTimestamp(v uint64) {
	s.Struct.SetUint64(24, v)
}

func (s TlogBlock) Operation() uint8 {
	return s.Struct.Uint8(32)
}

func (s TlogBlock) SetOperation(v uint8) {
	s.Struct.SetUint8(32, v)
}

// TlogBlock_List is a list of TlogBlock.
type TlogBlock_List struct{ capnp.List }

// NewTlogBlock creates a new list of TlogBlock.
func NewTlogBlock_List(s *capnp.Segment, sz int32) (TlogBlock_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 40, PointerCount: 2}, sz)
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
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 4})
	return TlogAggregation{st}, err
}

func NewRootTlogAggregation(s *capnp.Segment) (TlogAggregation, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 4})
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

func (s TlogAggregation) VdiskID() (string, error) {
	p, err := s.Struct.Ptr(1)
	return p.Text(), err
}

func (s TlogAggregation) HasVdiskID() bool {
	p, err := s.Struct.Ptr(1)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) VdiskIDBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(1)
	return p.TextBytes(), err
}

func (s TlogAggregation) SetVdiskID(v string) error {
	return s.Struct.SetText(1, v)
}

func (s TlogAggregation) Blocks() (TlogBlock_List, error) {
	p, err := s.Struct.Ptr(2)
	return TlogBlock_List{List: p.List()}, err
}

func (s TlogAggregation) HasBlocks() bool {
	p, err := s.Struct.Ptr(2)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetBlocks(v TlogBlock_List) error {
	return s.Struct.SetPtr(2, v.List.ToPtr())
}

// NewBlocks sets the blocks field to a newly
// allocated TlogBlock_List, preferring placement in s's segment.
func (s TlogAggregation) NewBlocks(n int32) (TlogBlock_List, error) {
	l, err := NewTlogBlock_List(s.Struct.Segment(), n)
	if err != nil {
		return TlogBlock_List{}, err
	}
	err = s.Struct.SetPtr(2, l.List.ToPtr())
	return l, err
}

func (s TlogAggregation) Prev() ([]byte, error) {
	p, err := s.Struct.Ptr(3)
	return []byte(p.Data()), err
}

func (s TlogAggregation) HasPrev() bool {
	p, err := s.Struct.Ptr(3)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetPrev(v []byte) error {
	return s.Struct.SetData(3, v)
}

// TlogAggregation_List is a list of TlogAggregation.
type TlogAggregation_List struct{ capnp.List }

// NewTlogAggregation creates a new list of TlogAggregation.
func NewTlogAggregation_List(s *capnp.Segment, sz int32) (TlogAggregation_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 16, PointerCount: 4}, sz)
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

const schema_f4533cbae6e08506 = "x\xda\x8c\x94_H,e\x18\xc6\xdf\xe7{gV\x05" +
	"u\x1bv\x83\x94\xc0\x02\x83\x8a,\xad\xae\xc4\xf0\x0f\x06" +
	"*\x0a~kPt\x13\xd3\xfa\xed\x9fvwf\xddo" +
	"\xd4\x08B\x0a\xbc\x89@\x02\x0b\x0b\x02\x0d\x83\x82\xa4." +
	"4\"\x12\x14\x0c\x13\x0c\x0a,\xea&68xy\xe0" +
	"\xc0\xb9\x9f\xc3\xb7\xeb\xfe9\xeb9\x9cs7\xef\xc3\xb3" +
	";\xcf\xfb\xbc?f\xf0\x14cb\xc8\xeec\"\xd9o" +
	"G\xc2\xa1\x8b\x0fG\xfe{\xf7\xd6\xc7${`\x87\x91" +
	"\xf5\xf2\xe5\xcf#\x0b\xb7\xc9\x16mD/-\xa3\x17\xb1" +
	"u\x98\xc7\x0f\xb0\x01B\xf8o\xffG\xa7\xff\xf7\xfe\xb9" +
	"e\xech\xb2W<\x8f\xf2\x8b\x88=\xc5mD\xb1'" +
	"y\x95\x10\x1e\xcd|\xb5\xf1\xf7\xe5E\xd9\xb8E\xab\xfb" +
	"\x13N \xf6u\xc5\xbd\xc3\xdf\x13\xc2\xcdr\xcf\x8f\xfb" +
	"\x07\xef\xdchu[\xc6\xfd\x8a5\x83\x98\xac<\xceY" +
	"\xaf\x9b(\xe3\xa9\xb3\xcf\xde\xff\xe2\xd3\x9b-Q*\x96" +
	"=\xfbM\xc4\x8em\xf3\xdf\x87\xf6*\x0d\x84:\x99Q" +
	"\x05\xf7\x85\x80\xf3~\xfa\xad\xea\xf0|\xd2-z\xc5\xe1" +
	"\xd7\xf2~z\"\xefs27\x0f\xc8\xc7\xd9\"\xb2@" +
	"\xe4\x1c\xcc\x10\xc9}\x86<\x12p\x808\x8cx8L" +
	"$\x7fb\xc8\x13\x01G\x888\x04\x91s\xfc,\x91\xfc" +
	"\x85!\x7f\x13\x00\xc7\xc1D\xce\xafF;b\xc8s\x01" +
	"\xc7B\x1c\x16\x91sf\xc4\x13\x86\xfcC\xc0\xb19\x0e" +
	"\x9b\xc8\xf9=A$\xcf\x19\xf2\x1f\x01'\xf2D\x1c\x11" +
	"\"\xe7/#^0dY \xd4jiYyIE" +
	"D\xe8 \x81\x0e\xc2\xa8\x9fJi\x15\xd4\xc6\xa8\xce\xbe" +
	"\xa7\xeaC\xc6\xd5\x19t\x91@\x17!\xba\xe8\x06nm" +
	"\x08\x83lA\xe9\xc0-\x10\x8a5w\xe8\x17U\xc9\x0d" +
	"\xb2>\xc1C\x84\x04\"\x84\x07\xb4\x95P\xba\xaf\xe8{" +
	"Z\x99\xc2\xda\xeb\x85=c\xba\xe9g\xc8A\x81Z_" +
	"\x03f\x8f\xe7\x18rJ`T\x07n\xb0\xac!H@" +
	"P\xd3Z\xd0\xe8&\xcc3*\x99\xba\x9b\xdeo]{" +
	"\xff\x94\xeb-\xea\x8c\x9bS\x09\xf3k\x8d\xc0d\xe8\xac" +
	"gxu\x82H\x8e1\xe4l#\xc3\xb4\xd1&\x19r" +
	"\xde\xdc\x0c\xd5\x9b\xcd\x95\x88\xe4,C\xbe!\xb0\xb6\xa2" +
	"J:\xeb{h'\x81v\xc2\xda\xcabV\xe7\xa6'" +
	"\xd1I\x02\x9d\x840\x95-\xe9`A-Q_%p" +
	"\xbd\xba\xfb\xc745\x8d\xa7\xd3%\x956\xd5zD&" +
	"\xe6c\xf5\x98\x9f\x1b\x106\x19r\xbb\x11\xf3K\xa3m" +
	"1\xe4nS\xcc\x1d\xd3\xdf6C~'\xe00\xaal" +
	"}k\x16\xdae\xc8\x1f\x0c[\xa2\xca\xd6\x9ei\xff\x9b" +
	"+2kl5\x93\x19\xf5\xdc\x82\xaa\xadt\x170\xf7" +
	"\xc2\xa2\xb5\x83\xd1\xb7\xf3~2W?\xd4#\x8d\xef\x06" +
	"\xc1\x88\xd1bI\xad\xd49{\xa8\xfb\xe9\xa2\xef\xf15" +
	"\x88&\x1a\x109\xb0\xae(2\xbb=\xcd\x90/_?" +
	"V\x0bUw\x02\x00\x00\xff\xff\xfb\xdc\x1f\xf1"

func init() {
	schemas.Register(schema_f4533cbae6e08506,
		0x8cf178de3c82d431,
		0x98d11ae1c78a24d9,
		0xe0d4e6d68fa24ac0,
		0xe46ab5b4b619e094,
		0xee959a7d96c96641)
}
