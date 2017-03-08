package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestLBA(t *testing.T) {
	//Check if the new function creates the proper amount of LBAShards
	l := NewLBA("", NumberOfRecordsPerLBAShard, nil)
	if len(l.shards) != 1 {
		t.Error("Wrong number of LBAShards")
	}
	l = NewLBA("", NumberOfRecordsPerLBAShard+1, nil)
	if len(l.shards) != 2 {
		t.Error("Wrong number of LBAShards")
	}

	shard := &LBAShard{}
	fmt.Println(shard)
	var buf bytes.Buffer

	var nilHash Hash

	for _, h := range *shard {
		if h == nil {
			buf.Write(nilHash[:])
		} else {
			buf.Write(h[:])
		}
	}
	fmt.Println(hex.EncodeToString(buf.Bytes()))
}
