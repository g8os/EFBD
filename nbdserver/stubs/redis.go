package stubs

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type values map[interface{}]interface{}

//MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	values  values
	hvalues map[interface{}]values
	lock    sync.RWMutex
}

//Close implements redis.Conn.Close
func (c *MemoryRedis) Close() error { return nil }

//Err implements redis.Conn.Err
func (c *MemoryRedis) Err() error { return nil }

//Do implements redis.Conn.Do
// Supported commands:
//   GET, SET, DEL, EXISTS, HGET, HSET, HEXISTS
// Supported but ignored commands:
//   MULTI, EXEC
// An error is returned if another command is given
func (c *MemoryRedis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if strings.HasPrefix(commandName, "H") {
		if len(args) < 1 {
			err = errors.New("Insufficient parameters")
			return
		}
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	switch commandName {
	case "SET":
		reply, err = c.values.setCommand(args...)
	case "GET":
		reply, err = c.values.getCommand(args...)
	case "EXISTS":
		reply, err = c.values.existsCommand(args...)
	case "DEL":
		reply, err = c.values.delCommand(args...)
	case "HSET":
		reply, err = c.getHValues(args[0]).setCommand(args[1:]...)
	case "HGET":
		reply, err = c.getHValues(args[0]).getCommand(args[1:]...)
	case "HEXISTS":
		reply, err = c.getHValues(args[0]).existsCommand(args[1:]...)
	case "HDEL":
		reply, err = c.getHValues(args[0]).delCommand(args[1:]...)
	case "MULTI", "EXEC": //Ignore  for now
	default:
		err = fmt.Errorf("Command %s not implemented", commandName)
	}
	return
}

//getHValues returns values for a specific hash
func (c *MemoryRedis) getHValues(hash interface{}) (v values) {
	v, found := c.hvalues[hash]
	if !found {
		v = make(values)
		c.hvalues[hash] = v
	}
	return
}

//Send implements redis.Conn.Send
// Only a small subset are implemented for Send:
//  SET, DEL, HSET, HDEL
// Implemented but ignored:
//  MULTI, EXEC
func (c *MemoryRedis) Send(commandName string, args ...interface{}) (err error) {
	switch commandName {
	case "SET", "DEL", "HSET", "HDEL":
		_, err = c.Do(commandName, args...)
	case "MULTI", "EXEC": //Ignore these for now
	default:
		err = fmt.Errorf("Command %s not implemented in Send", commandName)
	}
	return
}

func (v values) setCommand(args ...interface{}) (reply interface{}, err error) {
	if len(args) < 2 {
		err = errors.New("Insufficient parameters")
		return
	}
	key := fmt.Sprint(args[0])
	if byteSliceValue, ok := args[1].([]byte); ok {
		copyOfValue := make([]byte, len(byteSliceValue), len(byteSliceValue))
		copy(copyOfValue, byteSliceValue)
		args[1] = copyOfValue
	}
	v[key] = args[1]
	return
}

func (v values) getCommand(args ...interface{}) (reply interface{}, err error) {
	if len(args) < 1 {
		err = errors.New("Insufficient parameters")
		return
	}
	key := fmt.Sprint(args[0])
	reply = v[key]
	return
}

func (v values) existsCommand(args ...interface{}) (reply interface{}, err error) {
	if len(args) < 1 {
		err = errors.New("Insufficient parameters")
		return
	}
	key := fmt.Sprint(args[0])
	_, exists := v[key]
	if exists {
		reply = int64(1)
	} else {
		reply = int64(0)
	}
	return
}

func (v values) delCommand(args ...interface{}) (reply interface{}, err error) {
	if len(args) < 1 {
		err = errors.New("Insufficient parameters")
		return
	}
	key := fmt.Sprint(args[0])
	_, exists := v[key]
	if exists {
		reply = int64(1)
	} else {
		reply = int64(0)
	}
	delete(v, key)
	return
}

//Flush implements redis.Conn.Flush()
func (c *MemoryRedis) Flush() error { return nil }

//Receive implements redis.Conn.Receive
// Not implemented, always returns an error
func (c *MemoryRedis) Receive() (reply interface{}, err error) {
	err = errors.New("Receive is not implemented")
	return
}
