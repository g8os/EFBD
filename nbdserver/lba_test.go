package main

import "testing"

func TestLBA(t *testing.T) {
	//Check if the new function creates the proper amount of LBAShards
	l := NewLBA(NumberOfRecordsPerLBAShard)
	if len(*l) != 1 {
		t.Error("Wrong number of LBAShards")
	}
	l = NewLBA(NumberOfRecordsPerLBAShard + 1)
	if len(*l) != 2 {
		t.Error("Wrong number of LBAShards")
	}
}
