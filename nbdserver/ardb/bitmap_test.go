package ardb

import (
	"testing"
)

func TestBitMapTest(t *testing.T) {
	b := newBitMap()
	b.Set(2)
	if !b.Test(2) {
		t.Error("expected true, got false")
	}
	if b.Test(3) {
		t.Error("expected false, got true")
	}
	b.Set(3)
	if !b.Test(3) {
		t.Error("expected true, got false")
	}

	if b.Test(8096) {
		t.Error("expected false, got true")
	}
	b.Set(8096)
	if !b.Test(8096) {
		t.Error("expected true, got false")
	}
}

// Setting a value twice should not unset it.
func TestBitMapSetTwice(t *testing.T) {
	b := newBitMap()
	b.Set(2)
	if !b.Test(2) {
		t.Error("expected it to be set")
	}
	b.Set(2)
	if !b.Test(2) {
		t.Error("expected it to still be set")
	}
}

// Unset a value.
func TestSetUnset(t *testing.T) {
	b := newBitMap()

	b.Set(2)
	if !b.Test(2) {
		t.Error("expected it to be set")
	}
	b.Unset(2)
	if b.Test(2) {
		t.Error("expected it to be unset")
	}
	b.Unset(2)
	if b.Test(2) {
		t.Error("expected it to still be unset")
	}
}

func TestBitMapBytes(t *testing.T) {
	one := func(i int) bool {
		if i < 1024 {
			return i%2 == 1
		}
		if i < 4096 {
			return i%2 == 0
		}
		if i < 10000 {
			return true
		}
		return false
	}

	b := newBitMap()

	const (
		l = 12512
	)

	for i := 0; i < l; i++ {
		if one(i) {
			b.Set(i)
			if !b.Test(i) {
				t.Errorf("expected %d to be set", i)
			}
		} else {
			b.Unset(i)
			if b.Test(i) {
				t.Errorf("expected %d to be unset", i)
			}
		}
	}

	bytes, err := b.Bytes()
	if err != nil {
		t.Fatalf("expected Bytes to succeed: %v", err)
	}

	err = b.SetBytes(bytes)
	if err != nil {
		t.Fatalf("expected SetBytes to succeed: %v", err)
	}

	// test if setting bytes works,
	// even if bytes were already set
	for i := 0; i < l; i++ {
		if one(i) {
			if !b.Test(i) {
				t.Errorf("expected %d to be set", i)
			}
		} else {
			if b.Test(i) {
				t.Errorf("expected %d to be unset", i)
			}
		}
	}

	c := newBitMap()
	err = c.SetBytes(bytes)
	if err != nil {
		t.Fatalf("expected SetBytes to succeed: %v", err)
	}

	// test if creating from bytes works
	for i := 0; i < l; i++ {
		if one(i) {
			if !c.Test(i) {
				t.Errorf("expected %d to be set", i)
			}
		} else {
			if c.Test(i) {
				t.Errorf("expected %d to be unset", i)
			}
		}
	}

	// test if setting a partial bytes map overides everything
	b = newBitMap()
	for i := 0; i < 1024; i++ {
		b.Set(i)
		if !b.Test(i) {
			t.Errorf("expected %d to be set", i)
		}
	}
	bytes, err = b.Bytes()
	if err != nil {
		t.Fatalf("expected Bytes to succeed: %v", err)
	}

	err = c.SetBytes(bytes)
	if err != nil {
		t.Fatalf("expected SetBytes to succeed: %v", err)
	}

	one = func(i int) bool {
		return i < 1024
	}
	for i := 0; i < l; i++ {
		if one(i) {
			if !c.Test(i) {
				t.Errorf("expected %d to be set", i)
			}
		} else {
			if c.Test(i) {
				t.Errorf("expected %d to be unset", i)
			}
		}
	}
}
