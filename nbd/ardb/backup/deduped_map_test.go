package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnpackRawDedupedMapFailure(t *testing.T) {
	require := require.New(t)

	dm, err := UnpackRawDedupedMap(RawDedupedMap{Count: 1})
	require.Error(err, "should be error, as count is wrong")
	require.Nil(dm)

	dm, err = UnpackRawDedupedMap(RawDedupedMap{Count: 1, Indices: []int64{1, 2}})
	require.Error(err, "should be error, as index- and hash count is wrong")
	require.Nil(dm)

	dm, err = UnpackRawDedupedMap(RawDedupedMap{Count: 2, Indices: []int64{1, 2}, Hashes: [][]byte{[]byte{}}})
	require.Error(err, "should be error, as hash count is wrong")
	require.Nil(dm)

	dm, err = UnpackRawDedupedMap(RawDedupedMap{Count: 1, Indices: []int64{1, 2}, Hashes: [][]byte{[]byte{}}})
	require.Error(err, "should be error, as index count is wrong")
	require.Nil(dm)
}

func TestRawDedupedMap(t *testing.T) {
	require := require.New(t)

	originalRaw := RawDedupedMap{
		Count:   4,
		Indices: []int64{8, 4, 2, 1},
		Hashes: [][]byte{
			[]byte{1, 2},
			[]byte{2, 3},
			[]byte{3, 4},
			[]byte{4, 5},
		},
	}

	dm, err := UnpackRawDedupedMap(originalRaw)
	require.NoError(err)
	require.NotNil(dm)

	newRaw, err := dm.Raw()
	require.NoError(err)
	require.Equal(originalRaw.Count, newRaw.Count)
	require.Subset(originalRaw.Hashes, newRaw.Hashes)
	require.Subset(originalRaw.Indices, newRaw.Indices)
}
