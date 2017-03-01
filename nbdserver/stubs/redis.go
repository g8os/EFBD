package stubs

import (
	"errors"
	"fmt"
	"sync"
)

//MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	values map[interface{}]interface{}
	lock   sync.RWMutex
}

//Close implements redis.Conn.Close
func (c *MemoryRedis) Close() error { return nil }

//Err implements redis.Conn.Err
func (c *MemoryRedis) Err() error { return nil }

//Do implements redis.Conn.Do
// Only 'GET' and 'SET' commands are implemented, and error is returned if another commandName is given
func (c *MemoryRedis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch commandName {
	case "SET":
		if len(args) < 2 {
			err = errors.New("Insufficient parameters")
			return
		}
		if byteSliceValue, ok := args[1].([]byte); ok {
			copyOfValue := make([]byte, len(byteSliceValue), len(byteSliceValue))
			copy(copyOfValue, byteSliceValue)
			args[1] = copyOfValue
		}
		c.values[args[0]] = args[1]
	case "GET":
		if len(args) < 1 {
			err = errors.New("Insufficient parameters")
			return
		}
		reply = c.values[args[0]]
	default:
		err = fmt.Errorf("Command %s not implemented", commandName)
	}
	return
}

//Send implements redis.Conn.Send
// Not implemented, always returns an error
func (c *MemoryRedis) Send(commandName string, args ...interface{}) (err error) {
	switch commandName {
	case "SET":
		_, err = c.Do(commandName, args...)
	default:
		err = fmt.Errorf("Command %s not implemented in Send", commandName)
	}
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
