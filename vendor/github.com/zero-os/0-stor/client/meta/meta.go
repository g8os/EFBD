// Package meta  is WIP package for metadata.
// The spec need to be fixed first for further development.
package meta

import (
	"bytes"
	"io"
	"time"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-stor/client/meta/schema"
)

// Chunk is a the metadata of part of a file
type Chunk struct {
	Size   uint64   `json:"size"`
	Key    []byte   `json:"key"`
	Shards []string `json:"shards"`
}

// encode encodes the chunk into a canpn message
func (c *Chunk) encode(chunk schema.Metadata_Chunk) error {
	chunk.SetKey(c.Key)
	chunk.SetSize(c.Size)

	shardList, err := chunk.NewShards(int32(len(c.Shards)))
	if err != nil {
		return err
	}

	for i, shard := range c.Shards {
		if err := shardList.Set(i, shard); err != nil {
			return err
		}
	}

	return nil
}

// decode populate the Chunk object from a capnp Metadata_Chunk
func decodeChunk(chunk schema.Metadata_Chunk) (*Chunk, error) {
	c := &Chunk{}

	var err error

	c.Key, err = chunk.Key()
	if err != nil {
		return nil, err
	}
	c.Size = chunk.Size()

	shardList, err := chunk.Shards()
	if err != nil {
		return nil, err
	}

	c.Shards = make([]string, shardList.Len())
	for i := 0; i < shardList.Len(); i++ {
		c.Shards[i], err = shardList.At(i)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Meta defines a metadata for 0-stor
type Meta struct {
	Epoch     int64    `json:"epoch"`
	Key       []byte   `json:"key"`
	EncrKey   []byte   `json:"encryption_key"`
	Chunks    []*Chunk `json:"chunks"`
	Previous  []byte   `json:"previous"`
	Next      []byte   `json:"next"`
	ConfigPtr []byte   `json:"configPtr"`
}

// New creates new metadata
func New(key []byte) *Meta {

	meta := &Meta{
		Key:    key,
		Epoch:  time.Now().UnixNano(), //FIXME: do we need an uint64 here since Nanosecond returns a int anyway?
		Chunks: []*Chunk{},
	}

	return meta
}

func (m *Meta) Size() uint64 {
	var size uint64
	for _, chunk := range m.Chunks {
		size += chunk.Size
	}
	return size
}

// GetChunk return a chunk by it's key.
// if the key is not found, a new chunk is added to the metadata object and returned
func (m *Meta) GetChunk(key []byte) *Chunk {
	for i := range m.Chunks {
		chunk := m.Chunks[i]
		if bytes.Compare(chunk.Key, key) == 0 {
			return chunk
		}
	}
	chunk := &Chunk{Key: key, Shards: []string{}}
	m.Chunks = append(m.Chunks, chunk)
	return chunk
}

func (m *Meta) createCapnpMsg() (*capnp.Message, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	md, err := schema.NewRootMetadata(seg)
	if err != nil {
		return nil, err
	}

	err = md.SetKey(m.Key)
	if err != nil {
		return nil, err
	}
	err = md.SetEncrKey(m.EncrKey)
	if err != nil {
		return nil, err
	}
	md.SetEpoch(m.Epoch)
	err = md.SetPrevious(m.Previous)
	if err != nil {
		return nil, err
	}
	err = md.SetNext(m.Next)
	if err != nil {
		return nil, err
	}
	err = md.SetConfigPtr(m.ConfigPtr)
	if err != nil {
		return nil, err
	}

	chunkList, err := md.NewChunks(int32(len(m.Chunks)))
	if err != nil {
		return nil, err
	}

	for i := 0; i < chunkList.Len(); i++ {
		capnpChunk := chunkList.At(i)
		chunk := m.Chunks[i]
		if err := chunk.encode(capnpChunk); err != nil {
			return nil, err
		}
		if err := chunkList.Set(i, capnpChunk); err != nil {
			return nil, err
		}
	}

	return msg, nil
}

// Encode encodes the meta to capnp format
func (m *Meta) Encode(w io.Writer) error {
	msg, err := m.createCapnpMsg()
	if err != nil {
		return err
	}
	return capnp.NewEncoder(w).Encode(msg)
}

// EncodePacked encodes the meta to capnp format
func (m *Meta) EncodePacked(w io.Writer) error {
	msg, err := m.createCapnpMsg()
	if err != nil {
		return err
	}
	return capnp.NewPackedEncoder(w).Encode(msg)
}

// Bytes returns []byte representation of this meta
// in capnp format
func (m Meta) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := m.Encode(buf)
	return buf.Bytes(), err
}

// DecodeReader decodes from given io.Reader and returns
// the parsed Meta
func decodeMeta(msg *capnp.Message) (*Meta, error) {
	var (
		meta = &Meta{}
		err  error
	)

	md, err := schema.ReadRootMetadata(msg)
	if err != nil {
		return nil, err
	}

	meta.Epoch = md.Epoch()
	meta.Key, err = md.Key()
	if err != nil {
		return nil, err
	}
	meta.EncrKey, err = md.EncrKey()
	if err != nil {
		return nil, err
	}
	meta.Previous, err = md.Previous()
	if err != nil {
		return nil, err
	}
	meta.Next, err = md.Next()
	if err != nil {
		return nil, err
	}
	meta.ConfigPtr, err = md.ConfigPtr()
	if err != nil {
		return nil, err
	}

	chunkList, err := md.Chunks()
	if err != nil {
		return nil, err
	}

	meta.Chunks = make([]*Chunk, chunkList.Len())
	for i := 0; i < chunkList.Len(); i++ {
		capnpChunk := chunkList.At(i)
		if err != nil {
			return nil, err
		}
		chunk, err := decodeChunk(capnpChunk)
		if err != nil {
			return nil, err
		}
		meta.Chunks[i] = chunk
	}

	return meta, nil
}

// Decode decodes capnp encoded metadata
func Decode(p []byte) (*Meta, error) {
	msg, err := capnp.NewDecoder(bytes.NewReader(p)).Decode()
	if err != nil {
		return nil, err
	}

	return decodeMeta(msg)
}

// DecodePacked decodes packed capnp encoded metadata
func DecodePacked(p []byte) (*Meta, error) {
	msg, err := capnp.NewPackedDecoder(bytes.NewReader(p)).Decode()
	if err != nil {
		return nil, err
	}

	return decodeMeta(msg)
}
