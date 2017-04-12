//Package stubs provides fake implementations for external components like ardb.
// This can be used to test the performance of the nbdserver itself or for development purposes
package stubs

import "github.com/garyburd/redigo/redis"

//NewMemoryRedisConn creates a redis connection that stores everything in memory
func NewMemoryRedisConn() (conn redis.Conn) {
	return &MemoryRedis{
		values:  make(values),
		hvalues: make(map[interface{}]values),
	}
}
