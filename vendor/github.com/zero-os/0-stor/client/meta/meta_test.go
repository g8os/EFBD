package meta

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ugorji/go/codec"
	"github.com/zero-os/0-stor/client/meta/schema"

	"github.com/golang/protobuf/proto"
)

// func TestMarshalJSONMeta(t *testing.T) {
// 	meta, err := New([]byte("testkey"), 1024, []string{
// 		"http://127.0.0.1:12345",
// 		"http://127.0.0.1:12346",
// 		"http://127.0.0.1:12347",
// 	})
// 	meta.SetNumOfChunks(3)
// 	meta.SetPrevious([]byte("previous"))
// 	meta.SetNext([]byte("next"))
// 	meta.SetEncrKey([]byte("secret"))
// 	now := uint64(time.Now().UnixNano())
// 	meta.SetEpoch(now)

// 	b, err := json.Marshal(meta)
// 	require.NoError(t, err, "fail to json encode the metadata")
// 	expected := fmt.Sprintf(`{"configPtr":null,"encrKey":"c2VjcmV0","epoch":%d,"key":"dGVzdGtleQ==","next":"bmV4dA==","numOfChunks":3,"previous":"cHJldmlvdXM=","shard":["http://127.0.0.1:12345","http://127.0.0.1:12346","http://127.0.0.1:12347"],"size":1024}`, now)
// 	assert.EqualValues(t, strings.TrimSpace(expected), strings.TrimSpace(string(b)))
// }

func TestMetaSize(t *testing.T) {
	t.Run("capnp", func(t *testing.T) {
		meta := createCapnpMeta(t)

		capnpBuf := &bytes.Buffer{}
		err := meta.Encode(capnpBuf)
		require.NoError(t, err)
		fmt.Printf("size capnp: %d\n", len(capnpBuf.Bytes()))
	})

	t.Run("capnp-packed", func(t *testing.T) {
		meta := createCapnpMeta(t)

		capnpBuf := &bytes.Buffer{}
		err := meta.EncodePacked(capnpBuf)
		require.NoError(t, err)
		fmt.Printf("size capnp-packed: %d\n", len(capnpBuf.Bytes()))
	})

	t.Run("protobuf", func(t *testing.T) {
		meta := createProtobufMeta(t)

		b, err := proto.Marshal(meta)
		require.NoError(t, err)
		fmt.Printf("size protobuf: %d\n", len(b))
	})

	t.Run("msgpack", func(t *testing.T) {
		meta := createProtobufMeta(t)

		var (
			w  io.Writer
			b  []byte
			mh codec.MsgpackHandle
		)

		enc := codec.NewEncoder(w, &mh)
		enc = codec.NewEncoderBytes(&b, &mh)
		err := enc.Encode(meta)
		require.NoError(t, err)
		fmt.Printf("size msgpack: %d\n", len(b))
	})
}

func createCapnpMeta(t testing.TB) *Meta {
	chunks := make([]*Chunk, 256)
	for i := range chunks {
		chunks[i] = &Chunk{
			Key:  []byte(fmt.Sprintf("chunk%d", i)),
			Size: 1024,
		}
		chunks[i].Shards = make([]string, 5)
		for y := range chunks[i].Shards {
			chunks[i].Shards[y] = fmt.Sprintf("http://127.0.0.1:12345/stor-%d", i)
		}
	}

	meta := New([]byte("testkey"))
	meta.Previous = []byte("previous")
	meta.Next = []byte("next")
	meta.EncrKey = []byte("secret")
	meta.Chunks = chunks

	return meta
}

func createProtobufMeta(t testing.TB) *schema.ProtoMetadata {
	meta := &schema.ProtoMetadata{}
	meta.Key = []byte("testkey")
	meta.Size = 1024
	meta.Shards = make([]string, 256)
	for i := range meta.Shards {
		meta.Shards[i] = fmt.Sprintf("http://127.0.0.1:12345/stor-%d", i)
	}
	meta.NumChunks = 3
	meta.Previous = []byte("previous")
	meta.Next = []byte("next")
	meta.Epoch = uint64(time.Now().Nanosecond())
	meta.EncrKey = []byte("secret")

	return meta
}

func BenchmarkEncoding(b *testing.B) {
	b.Run("capnp", func(b *testing.B) {
		meta := createCapnpMeta(b)
		buf := &bytes.Buffer{}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := meta.Encode(buf)
			if err != nil {
				b.Error(err)
			}
			buf.Reset()
		}
	})

	b.Run("capnp-packed", func(b *testing.B) {
		meta := createCapnpMeta(b)
		buf := &bytes.Buffer{}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := meta.EncodePacked(buf)
			if err != nil {
				b.Error(err)
			}
			buf.Reset()
		}
	})

	b.Run("protobuf", func(b *testing.B) {
		meta := createProtobufMeta(b)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			encoded, err := proto.Marshal(meta)
			if err != nil {
				b.Error(err)
			}
			_ = encoded
		}
	})

	b.Run("protobuf-buffer", func(b *testing.B) {
		var (
			meta = createProtobufMeta(b)
			buf  = proto.Buffer{}
			err  error
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = buf.EncodeMessage(meta)
			if err != nil {
				b.Error(err)
			}
			buf.Reset()
		}
	})

	b.Run("msgpack", func(b *testing.B) {
		var (
			meta = createProtobufMeta(b)
			w    io.Writer
			blob []byte
			mh   codec.MsgpackHandle
			err  error
		)

		enc := codec.NewEncoder(w, &mh)
		enc = codec.NewEncoderBytes(&blob, &mh)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = enc.Encode(meta)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
