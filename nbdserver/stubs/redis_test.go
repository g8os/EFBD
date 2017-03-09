package stubs

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestMemoryRedis(t *testing.T) {
	c := NewMemoryRedisConn()
	if c == nil {
		t.Error("No connection created")
		return
	}
	//Test via Do
	c.Do("SET", "key1", "value1")
	r, err := c.Do("GET", "key1")
	if err != nil || r == nil || r != "value1" {
		t.Error("Invalid reply")
	}
	numberOfKeys, err := redis.Int(c.Do("EXISTS", "key1"))
	if err != nil || numberOfKeys != 1 {
		t.Error("Invalid reply", err)
	}

	//Test 'SET' via Send
	c.Send("SET", "key2", "value2")
	c.Flush()
	r, err = c.Do("GET", "key2")
	if err != nil || r == nil || r != "value2" {
		t.Error("Invalid reply, err:", err, "reply:", r)
	}

	//Test "EXISTS" with unexisting key
	numberOfKeys, err = redis.Int(c.Do("EXISTS", "nosuchkey"))
	if err != nil || numberOfKeys != 0 {
		t.Error("Invalid reply", err)
	}
}
