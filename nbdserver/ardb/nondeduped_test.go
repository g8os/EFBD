package ardb

import "testing"

func BenchmarkTimeOfKeyCreation(b *testing.B) {
	s := &nonDedupedStorage{
		vdiskID: "MyVdiskID",
	}
	for i := 0; i < b.N; i++ {
		s.getKey(int64(i))
	}

}
