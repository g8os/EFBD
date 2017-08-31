package db

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectEncodeDecode(t *testing.T) {
	data := make([]byte, 1024)
	obj := NewObject(data)

	_, err := rand.Read(obj.Data)
	require.NoError(t, err)

	copy(obj.ReferenceList[0][:], []byte("user1"))
	copy(obj.ReferenceList[1][:], []byte("user2"))

	b, err := obj.Encode()
	require.NoError(t, err)

	obj2 := NewObject(nil)
	err = obj2.Decode(b)
	require.NoError(t, err)

	assert.Equal(t, obj.ReferenceList, obj2.ReferenceList, "reference list differs")
	assert.Equal(t, obj.Data, obj2.Data, "data differes")
}

func TestNamespaceEncodeDecode(t *testing.T) {
	ns := Namespace{
		Label:    "mynamespace",
		Reserved: 1024 * 1024,
	}

	b, err := ns.Encode()
	require.NoError(t, err)

	ns2 := Namespace{}
	err = ns2.Decode(b)
	require.NoError(t, err)

	assert.Equal(t, ns.Label, ns2.Label, "label differs")
	assert.Equal(t, ns.Reserved, ns2.Reserved, "reserved differs")

}

func TestStoreStatEncodeDecode(t *testing.T) {
	stat := StoreStat{
		SizeAvailable: 100,
		SizeUsed:      45,
	}

	b, err := stat.Encode()
	require.NoError(t, err, "fail to encode StoreStat")

	stat2 := StoreStat{}
	err = stat2.Decode(b)
	require.NoError(t, err, "fail to decode StoreStat")

	assert.Equal(t, stat, stat2, "two object should be the same")
}

func TestObjectValidateCRC(t *testing.T) {
	data := make([]byte, 1024)
	obj := NewObject(data)

	_, err := rand.Read(obj.Data)
	require.NoError(t, err)

	copy(obj.ReferenceList[0][:], []byte("user1"))
	copy(obj.ReferenceList[1][:], []byte("user2"))

	b, err := obj.Encode()
	require.NoError(t, err)

	obj2 := NewObject(nil)
	err = obj2.Decode(b)
	require.NoError(t, err)

	assert.True(t, obj2.ValidCRC(), "CRC should be valid")
	for i := 0; i < 10; i++ {
		obj2.Data[i] = -obj2.Data[i] // corrupte the data
	}
	assert.False(t, obj2.ValidCRC(), "CRC should be different")
}

func BenchmarkObjectEncode(b *testing.B) {
	data := make([]byte, 1024)
	obj := NewObject(data)

	_, err := rand.Read(obj.Data)
	require.NoError(b, err)

	for i := range obj.ReferenceList {
		copy(obj.ReferenceList[i][:], []byte(fmt.Sprintf("user%d", i)))
	}

	b.ResetTimer()
	var x []byte
	for i := 0; i < b.N; i++ {
		x, err = obj.Encode()
		_ = x
	}
}
