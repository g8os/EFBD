package stubs

import "testing"

func TestMemoryRedis(t *testing.T) {
	c := NewMemoryRedisConn()
	if c == nil {
		t.Error("No connection created")
		return
	}
	//Test 'SET' via Do
	c.Do("SET", "key1", "value1")
	r, err := c.Do("GET", "key1")
	if err != nil || r == nil || r != "value1" {
		t.Error("Invalid reply")
	}

	//Test 'SET' via Send
	c.Send("SET", "key2", "value2")
	c.Flush()
	r, err = c.Do("GET", "key2")
	if err != nil || r == nil || r != "value2" {
		t.Error("Invalid reply, err:", err, "reply:", r)
	}
}
