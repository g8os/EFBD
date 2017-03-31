package ardb

import "testing"

func BenchmarkTimeOfKeyCreation(b *testing.B) {
	s := &nonDedupedStorage{
		volumeID: "MyVolumeID",
	}
	for i := 0; i < b.N; i++ {
		s.getKey(int64(i))
	}

}
