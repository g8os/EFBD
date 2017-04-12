package redisstub

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/garyburd/redigo/redis"

	lediscfg "github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
)

// Goals
//	+ InMemory;
//	+ Support all Redis Commands;
//	+ Minimal configuration;
//	+ Lua Support;

// TODO:
// + Missing Key Commands:
//     expire, expireat, keys, migrate, move, object, persist,
//     pexpire, pexpireat, pttl, randomkkey, rename, renamenx, restore,
//     sort, touch, ttl, type, unlink, wait, scan
// + Missing set commands:
//     smove, spop, srandmember, sscan
// + Missing hashes commands:
//     hincrbyfloat, hsetnx, hstrlen, hscan
// + Missing list commands;
// + Mising Script Commands:
//     eval, evalsha, scriptdebug, scriptexists, scriptflush, scriptkill, scriptload

// NewMemoryRedis creates a redis connection that stores everything in memory
// TODO: Improve configuration
func NewMemoryRedis() (conn redis.Conn) {
	cfg := lediscfg.NewConfigDefault()
	cfg.Addr = ""
	cfg.DBName = "memory"
	cfg.DataDir, _ = ioutil.TempDir("", "redisstub")

	l, _ := ledis.Open(cfg)
	db, _ := l.Select(0)

	return &MemoryRedis{
		db: db,
		tx: newTransaction(),
	}
}

//MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	db *ledis.DB
	tx *transaction
}

//Close implements redis.Conn.Close
func (c *MemoryRedis) Close() error {
	return c.tx.Clear()
}

//Err implements redis.Conn.Err
func (c *MemoryRedis) Err() error {
	return nil
}

//Do implements redis.Conn.Do
func (c *MemoryRedis) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	switch strings.ToUpper(commandName) {
	// key commands
	case "SET":
		reply, err = c.set(args...)
	case "GET":
		reply, err = c.get(args...)
	case "DEL":
		reply, err = c.del(args...)
	case "DUMP":
		reply, err = c.dump(args...)
	case "EXISTS":
		reply, err = c.exists(args...)
	case "INCR":
		reply, err = c.incr(args...)
	case "INCRBY":
		reply, err = c.incrBy(args...)
	case "DECR":
		reply, err = c.decr(args...)
	case "DECRBY":
		reply, err = c.decrBy(args...)

	// hashes commands
	case "HDEL":
		reply, err = c.hdel(args...)
	case "HEXISTS":
		reply, err = c.hexists(args...)
	case "HGET":
		reply, err = c.hget(args...)
	case "HGETALL":
		reply, err = c.hgetall(args...)
	case "HINCRBY":
		reply, err = c.hincrby(args...)
	case "HKEYS":
		reply, err = c.hkeys(args...)
	case "HLEN":
		reply, err = c.hlen(args...)
	case "HMGET":
		reply, err = c.hmget(args...)
	case "HMSET":
		reply, err = c.hmset(args...)
	case "HSET":
		reply, err = c.hset(args...)
	case "HVALS":
		reply, err = c.hvals(args...)

	// set commands
	case "SADD":
		reply, err = c.sadd(args...)
	case "SCARD":
		reply, err = c.scard(args...)
	case "SDIFF":
		reply, err = c.sdiff(args...)
	case "SDIFFSTORE":
		reply, err = c.sdiffstore(args...)
	case "SINTER":
		reply, err = c.sinter(args...)
	case "SINTERSTORE":
		reply, err = c.sinterstore(args...)
	case "SISMEMBER":
		reply, err = c.sismember(args...)
	case "SMEMBERS":
		reply, err = c.smembers(args...)
	case "SREM":
		reply, err = c.srem(args...)
	case "SUNION":
		reply, err = c.sunion(args...)
	case "SUNIONSTORE":
		reply, err = c.sunionstore(args...)

	// script commands

	// transaction commands
	case "MULTI":
		err = errors.New("MULTI Command should be done as a send command, not a Do Command")
	case "DISCARD":
		reply, err = c.discard(args...)
	case "EXEC":
		reply, err = c.exec(args...)

	// other (not supported?)
	default:
		err = fmt.Errorf("Command %s not implemented", commandName)
	}

	return
}

//Send implements redis.Conn.Send
func (c *MemoryRedis) Send(commandName string, args ...interface{}) (err error) {
	switch strings.ToUpper(commandName) {
	// key commands
	case "SET":
		err = c.tx.Add(c.set, 2, args...)
	case "GET":
		err = c.tx.Add(c.get, 1, args...)
	case "DEL":
		err = c.tx.Add(c.del, 1, args...)
	case "DUMP":
		err = c.tx.Add(c.dump, 1, args...)
	case "EXISTS":
		err = c.tx.Add(c.exists, 1, args...)
	case "INCR":
		err = c.tx.Add(c.incr, 1, args...)
	case "INCRBY":
		err = c.tx.Add(c.incrBy, 2, args...)
	case "DECR":
		err = c.tx.Add(c.decr, 1, args...)
	case "DECRBY":
		err = c.tx.Add(c.decrBy, 2, args...)

	// hashes commands
	case "HDEL":
		err = c.tx.Add(c.hdel, 2, args...)
	case "HEXISTS":
		err = c.tx.Add(c.hexists, 2, args...)
	case "HGET":
		err = c.tx.Add(c.hget, 2, args...)
	case "HGETALL":
		err = c.tx.Add(c.hgetall, 1, args...)
	case "HINCRBY":
		err = c.tx.Add(c.hincrby, 3, args...)
	case "HKEYS":
		err = c.tx.Add(c.hkeys, 1, args...)
	case "HLEN":
		err = c.tx.Add(c.hlen, 1, args...)
	case "HMGET":
		err = c.tx.Add(c.hmget, 2, args...)
	case "HMSET":
		err = c.tx.Add(c.hmset, 3, args...)
	case "HSET":
		err = c.tx.Add(c.hset, 3, args...)
	case "HVALS":
		err = c.tx.Add(c.hvals, 1, args...)

	// set commands
	case "SADD":
		err = c.tx.Add(c.sadd, 2, args...)
	case "SCARD":
		err = c.tx.Add(c.scard, 1, args...)
	case "SDIFF":
		err = c.tx.Add(c.sdiff, 1, args...)
	case "SDIFFSTORE":
		err = c.tx.Add(c.sdiffstore, 2, args...)
	case "SINTER":
		err = c.tx.Add(c.sinter, 1, args...)
	case "SINTERSTORE":
		err = c.tx.Add(c.sinterstore, 2, args...)
	case "SISMEMBER":
		err = c.tx.Add(c.sismember, 2, args...)
	case "SMEMBERS":
		err = c.tx.Add(c.smembers, 1, args...)
	case "SREM":
		err = c.tx.Add(c.srem, 2, args...)
	case "SUNION":
		err = c.tx.Add(c.sunion, 1, args...)
	case "SUNIONSTORE":
		err = c.tx.Add(c.sunionstore, 2, args...)

	// script commands

	// transaction commands
	case "MULTI":
		_, err = c.multi(args...)
	case "DISCARD":
		err = errors.New("DISCARD Command should be done as a Do command, not a Send Command")
	case "EXEC":
		err = errors.New("DISCARD Command should be done as a Do command, not a Send Command")

	// other (not supported?!)
	default:
		err = fmt.Errorf("Command %s not implemented", commandName)
	}

	return
}

//Flush implements redis.Conn.Flush()
func (c *MemoryRedis) Flush() (err error) {
	_, err = c.tx.Commit()
	return
}

//Receive implements redis.Conn.Receive
func (c *MemoryRedis) Receive() (reply interface{}, err error) {
	err = errors.New("Receive is not supported by go-redis-stub")
	return
}

func (c *MemoryRedis) set(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	value := asByteSlice(args[1])

	err = c.db.Set(key, value)
	if err != nil {
		reply = ok
	}
	return
}

func (c *MemoryRedis) get(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	value, err := c.db.Get(key)
	// redigo/redis returns nil when not found,
	// while ledisdb seems to return an empty slice,
	// so this conditional makes it redis compliant
	if err == nil && len(value) == 0 {
		reply = nil
	} else {
		reply = value
	}
	return
}

func (c *MemoryRedis) del(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	keys := make([][]byte, len(args))
	for i, arg := range args {
		keys[i] = asByteSlice(arg)
	}

	reply, err = c.db.Del(keys...)
	return
}

func (c *MemoryRedis) dump(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	value, err := c.db.Dump(key)
	// redigo/redis returns nil when not found,
	// while ledisdb seems to return an empty slice,
	// so this conditional makes it redis compliant
	if err == nil && len(value) == 0 {
		reply = nil
	} else {
		reply = value
	}
	return
}

func (c *MemoryRedis) exists(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.Exists(key)
	return
}

func (c *MemoryRedis) incr(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.Incr(key)
	return
}

func (c *MemoryRedis) incrBy(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	increment, err := asInt64(args[1])
	if err != nil {
		return
	}
	key := asByteSlice(args[0])

	reply, err = c.db.IncrBy(key, increment)
	return
}

func (c *MemoryRedis) decr(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.Decr(key)
	return
}

func (c *MemoryRedis) decrBy(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	increment, err := asInt64(args[1])
	if err != nil {
		return
	}
	key := asByteSlice(args[0])

	reply, err = c.db.DecrBy(key, increment)
	return
}

func (c *MemoryRedis) hdel(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	var fields [][]byte
	for _, arg := range args[1:] {
		fields = append(fields, asByteSlice(arg))
	}

	reply, err = c.db.HDel(key, fields...)
	return
}

func (c *MemoryRedis) hexists(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	field := asByteSlice(args[1])

	if value, err := c.db.HGet(key, field); err == nil && len(value) > 0 {
		reply = int64(1)
	} else {
		reply = int64(0)
	}

	return
}

func (c *MemoryRedis) hget(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	field := asByteSlice(args[1])

	value, err := c.db.HGet(key, field)
	// redigo/redis returns nil when not found,
	// while ledisdb seems to return an empty slice,
	// so this conditional makes it redis compliant
	if err == nil && len(value) == 0 {
		reply = nil
	} else {
		reply = value
	}
	return
}

func (c *MemoryRedis) hgetall(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	pairs, err := c.db.HGetAll(key)
	if err != nil {
		return
	}

	var replies []interface{}
	for _, pair := range pairs {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		value := pair.Value
		if len(value) == 0 {
			value = nil
		}
		replies = append(replies, pair.Field, value)
	}
	reply = replies

	return
}

func (c *MemoryRedis) hincrby(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 3 {
		err = &InsufficientArgumentsError{3, length}
		return
	}

	delta, err := asInt64(args[2])
	if err != nil {
		return
	}

	key := asByteSlice(args[0])
	field := asByteSlice(args[1])

	reply, err = c.db.HIncrBy(key, field, delta)
	return
}

func (c *MemoryRedis) hkeys(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.HKeys(key)
	return
}

func (c *MemoryRedis) hlen(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.HLen(key)
	return
}

func (c *MemoryRedis) hmget(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	var fields [][]byte
	for _, arg := range args[1:] {
		fields = append(fields, asByteSlice(arg))
	}

	values, err := c.db.HMget(key, fields...)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) hmset(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 3 {
		err = &InsufficientArgumentsError{3, length}
		return
	} else if (length-1)%2 != 0 {
		err = errors.New("arguments needs to be given in pairs of [field value]")
		return
	}

	key := asByteSlice(args[0])
	var pairs []ledis.FVPair
	for i := 1; i < len(args); i += 2 {
		field := asByteSlice(args[i])
		value := asByteSlice(args[i+1])
		pairs = append(pairs, ledis.FVPair{
			Field: field,
			Value: value,
		})
	}

	if err = c.db.HMset(key, pairs...); err != nil {
		return
	}

	reply = ok
	return
}

func (c *MemoryRedis) hset(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 3 {
		err = &InsufficientArgumentsError{3, length}
		return
	}

	key := asByteSlice(args[0])
	field := asByteSlice(args[1])
	value := asByteSlice(args[2])

	reply, err = c.db.HSet(key, field, value)
	return
}

func (c *MemoryRedis) hvals(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	values, err := c.db.HValues(key)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) sadd(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	var members [][]byte
	for _, arg := range args[1:] {
		members = append(members, asByteSlice(arg))
	}

	reply, err = c.db.SAdd(key, members...)
	return
}

func (c *MemoryRedis) scard(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	reply, err = c.db.SCard(key)
	return
}

func (c *MemoryRedis) sdiff(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	var keys [][]byte
	for _, arg := range args {
		keys = append(keys, asByteSlice(arg))
	}

	values, err := c.db.SDiff(keys...)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) sdiffstore(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	destKey := asByteSlice(args[0])
	var keys [][]byte
	for _, arg := range args[1:] {
		keys = append(keys, asByteSlice(arg))
	}

	reply, err = c.db.SDiffStore(destKey, keys...)
	return
}

func (c *MemoryRedis) sinter(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	var keys [][]byte
	for _, arg := range args {
		keys = append(keys, asByteSlice(arg))
	}

	values, err := c.db.SInter(keys...)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) sinterstore(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	destKey := asByteSlice(args[0])
	var keys [][]byte
	for _, arg := range args[1:] {
		keys = append(keys, asByteSlice(arg))
	}

	reply, err = c.db.SInterStore(destKey, keys...)
	return
}

func (c *MemoryRedis) sismember(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	member := asByteSlice(args[1])

	reply, err = c.db.SIsMember(key, member)
	return
}

func (c *MemoryRedis) smembers(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	key := asByteSlice(args[0])

	values, err := c.db.SMembers(key)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) srem(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	key := asByteSlice(args[0])
	var members [][]byte
	for _, arg := range args[1:] {
		members = append(members, asByteSlice(arg))
	}

	reply, err = c.db.SRem(key, members...)
	return
}

func (c *MemoryRedis) sunion(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 1 {
		err = &InsufficientArgumentsError{1, length}
		return
	}

	var keys [][]byte
	for _, arg := range args {
		keys = append(keys, asByteSlice(arg))
	}

	values, err := c.db.SUnion(keys...)
	for index := range values {
		// redigo/redis returns nil when not found,
		// while ledisdb seems to return an empty slice,
		// so this conditional makes it redis compliant
		if len(values[index]) == 0 {
			values[index] = nil
		}
	}
	reply = values
	return
}

func (c *MemoryRedis) sunionstore(args ...interface{}) (reply interface{}, err error) {
	if length := len(args); length < 2 {
		err = &InsufficientArgumentsError{2, length}
		return
	}

	destKey := asByteSlice(args[0])
	var keys [][]byte
	for _, arg := range args[1:] {
		keys = append(keys, asByteSlice(arg))
	}

	reply, err = c.db.SUnionStore(destKey, keys...)
	return
}

func (c *MemoryRedis) multi(args ...interface{}) (reply interface{}, err error) {
	if err = c.tx.Start(); err != nil {
		return
	}

	reply = ok
	return
}

func (c *MemoryRedis) discard(args ...interface{}) (reply interface{}, err error) {
	if err = c.tx.Discard(); err != nil {
		return
	}

	reply = ok
	return
}

func (c *MemoryRedis) exec(args ...interface{}) (reply interface{}, err error) {
	return c.tx.Execute()
}

// InsufficientArgumentsError is returned when
// a command has insufficient arguments and cannot be executed because of it.
type InsufficientArgumentsError struct {
	Expected, Received int
}

// Error implements error.Error
func (err *InsufficientArgumentsError) Error() string {
	return fmt.Sprintf(
		"insufficient arguments: expected %d, while received only %d",
		err.Expected, err.Received)
}

var (
	ok = []byte("OK")
)
