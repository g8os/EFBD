package stubs

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestNormalOperations(t *testing.T) {
	c := NewMemoryRedisConn()

	//Test via Do
	r, err := c.Do("SET", "key1", "value1")
	if err != nil {
		t.Error("EROR executing SET:", err)
	}
	r, err = c.Do("GET", "key1")
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

	//Test "DEL"
	c.Send("DEL", "key2")
	numberOfKeys, err = redis.Int(c.Do("EXISTS", "key2"))
	if err != nil || numberOfKeys != 0 {
		t.Error("Invalid reply in GET after DEL", err)
	}
}

func TestHOperations(t *testing.T) {
	c := NewMemoryRedisConn()

	h := "myhash"

	//Test via Do
	c.Do("HSET", h, "key1", "value1")
	r, err := c.Do("HGET", h, "key1")
	if err != nil || r == nil || r != "value1" {
		t.Error("Invalid reply")
	}
	numberOfKeys, err := redis.Int(c.Do("HEXISTS", h, "key1"))
	if err != nil || numberOfKeys != 1 {
		t.Error("Invalid reply", err)
	}

	//Test 'SET' via Send
	c.Send("HSET", h, "key2", "value2")
	c.Flush()
	r, err = c.Do("HGET", h, "key2")
	if err != nil || r == nil || r != "value2" {
		t.Error("Invalid reply, err:", err, "reply:", r)
	}

	//Test "HEXISTS" with unexisting key
	numberOfKeys, err = redis.Int(c.Do("HEXISTS", h, "nosuchkey"))
	if err != nil || numberOfKeys != 0 {
		t.Error("Invalid reply", err)
	}

	//Test "HEXISTS" with unexisting hash
	numberOfKeys, err = redis.Int(c.Do("HEXISTS", "differenthash", "key2"))
	if err != nil || numberOfKeys != 0 {
		t.Error("Invalid reply", err)
	}

	//Test "HDEL"
	c.Send("HDEL", h, "key2")
	numberOfKeys, err = redis.Int(c.Do("HEXISTS", h, "key2"))
	if err != nil || numberOfKeys != 0 {
		t.Error("Invalid reply in HGET after HDEL", err)
	}
}

func TestIgnoredCommands(t *testing.T) {
	c := NewMemoryRedisConn()
	ignoredCommands := []string{"MULTI", "EXEC"}
	for _, command := range ignoredCommands {
		if _, err := c.Do(command); err != nil {
			t.Errorf("Command %s should be ignored but gives error %s", command, err)
		}
		if err := c.Send(command); err != nil {
			t.Errorf("Command %s should be ignored but gives error %s", command, err)
		}
	}
}
