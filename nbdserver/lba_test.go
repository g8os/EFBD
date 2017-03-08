package main

import "testing"

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
}
