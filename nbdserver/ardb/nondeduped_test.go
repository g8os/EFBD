package ardb

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeOfKeyCreation(t *testing.T) {
	s := &nonDedupedStorage{
		volumeID: "MyVolumeID",
	}
	iterations := int64(1000 * 1000)
	start := time.Now()
	for i := int64(0); i < iterations; i++ {
		s.getKey(i)
	}
	took := time.Now().Sub(start)
	fmt.Printf("Creating %d keys took %f seconds", iterations, took.Seconds())

}
