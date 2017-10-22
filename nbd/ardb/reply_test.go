package ardb

import (
	"reflect"
	"testing"
)

type valueError struct {
	v   interface{}
	err error
}

func ve(v interface{}, err error) valueError {
	return valueError{v, err}
}

var replyTests = []struct {
	name     interface{}
	actual   valueError
	expected valueError
}{
	{
		`Bytes("hello")`,
		ve(Bytes("hello", nil)),
		ve([]byte("hello"), nil),
	},
	{
		`Bytes([1,2,3])`,
		ve(Bytes([]byte{1, 2, 3}, nil)),
		ve([]byte{1, 2, 3}, nil),
	},
	{
		`Bytes(nil)`,
		ve(Bytes(nil, nil)),
		ve([]byte(nil), ErrNil),
	},
	{
		`OptBytes("hello")`,
		ve(OptBytes("hello", nil)),
		ve([]byte("hello"), nil),
	},
	{
		`OptBytes([1,2,3])`,
		ve(OptBytes([]byte{1, 2, 3}, nil)),
		ve([]byte{1, 2, 3}, nil),
	},
	{
		`OptBytes(nil)`,
		ve(OptBytes(nil, nil)),
		ve([]byte(nil), nil),
	},
	{
		`Bool(int64(0))`,
		ve(Bool(int64(0), nil)),
		ve(false, nil),
	},
	{
		`Bool(int64(1))`,
		ve(Bool(int64(1), nil)),
		ve(true, nil),
	},
	{
		`Bool(int64(42))`,
		ve(Bool(int64(42), nil)),
		ve(true, nil),
	},
	{
		`Bool(nil)`,
		ve(Bool(nil, nil)),
		ve(false, ErrNil),
	},
	{
		`OptBool(int64(0))`,
		ve(OptBool(int64(0), nil)),
		ve(false, nil),
	},
	{
		`OptBool(int64(1))`,
		ve(OptBool(int64(1), nil)),
		ve(true, nil),
	},
	{
		`OptBool(int64(42))`,
		ve(OptBool(int64(42), nil)),
		ve(true, nil),
	},
	{
		`OptBool(nil)`,
		ve(OptBool(nil, nil)),
		ve(false, nil),
	},
	{
		`Int(int64(0))`,
		ve(Int(int64(0), nil)),
		ve(0, nil),
	},
	{
		`Int([]byte("123456789"))`,
		ve(Int([]byte("123456789"), nil)),
		ve(123456789, nil),
	},
	{
		`Int(int64(-666))`,
		ve(Int(int64(-666), nil)),
		ve(-666, nil),
	},
	{
		`Int(nil)`,
		ve(Int(nil, nil)),
		ve(0, ErrNil),
	},
	{
		`OptInt(int64(0))`,
		ve(OptInt(int64(0), nil)),
		ve(0, nil),
	},
	{
		`OptInt([]byte("123456789"))`,
		ve(OptInt([]byte("123456789"), nil)),
		ve(123456789, nil),
	},
	{
		`OptInt(int64(-666))`,
		ve(OptInt(int64(-666), nil)),
		ve(-666, nil),
	},
	{
		`OptInt(nil)`,
		ve(OptInt(nil, nil)),
		ve(0, nil),
	},
	{
		`Int64(0)`,
		ve(Int64(int64(0), nil)),
		ve(int64(0), nil),
	},
	{
		`Int64("123456789")`,
		ve(Int64([]byte("123456789"), nil)),
		ve(int64(123456789), nil),
	},
	{
		`Int64(-666)`,
		ve(Int64(int64(-666), nil)),
		ve(int64(-666), nil),
	},
	{
		`Int64(nil)`,
		ve(Int64(nil, nil)),
		ve(int64(0), ErrNil),
	},
	{
		`OptInt64(0)`,
		ve(OptInt64(int64(0), nil)),
		ve(int64(0), nil),
	},
	{
		`OptInt64("123456789")`,
		ve(OptInt64([]byte("123456789"), nil)),
		ve(int64(123456789), nil),
	},
	{
		`OptInt64(-666)`,
		ve(OptInt64(int64(-666), nil)),
		ve(int64(-666), nil),
	},
	{
		`OptInt64(nil)`,
		ve(OptInt64(nil, nil)),
		ve(int64(0), nil),
	},
	{
		`Uint64(0)`,
		ve(Uint64(int64(0), nil)),
		ve(uint64(0), nil),
	},
	{
		`Uint64("123456789")`,
		ve(Uint64([]byte("123456789"), nil)),
		ve(uint64(123456789), nil),
	},
	{
		`Uint64(nil)`,
		ve(Uint64(nil, nil)),
		ve(uint64(0), ErrNil),
	},
	{
		`OptUint64(0)`,
		ve(OptUint64(int64(0), nil)),
		ve(uint64(0), nil),
	},
	{
		`OptUint64("123456789")`,
		ve(OptUint64([]byte("123456789"), nil)),
		ve(uint64(123456789), nil),
	},
	{
		`OptUint64(nil)`,
		ve(OptUint64(nil, nil)),
		ve(uint64(0), nil),
	},
	{
		`String("hello")`,
		ve(String("hello", nil)),
		ve("hello", nil),
	},
	{
		`String([]byte("ok"))`,
		ve(String([]byte("ok"), nil)),
		ve("ok", nil),
	},
	{
		`String("✓")`,
		ve(String("✓", nil)),
		ve("✓", nil),
	},
	{
		`String(nil)`,
		ve(String(nil, nil)),
		ve("", ErrNil),
	},
	{
		`OptString("hello")`,
		ve(OptString("hello", nil)),
		ve("hello", nil),
	},
	{
		`OptString([]byte("ok"))`,
		ve(OptString([]byte("ok"), nil)),
		ve("ok", nil),
	},
	{
		`OptString("✓")`,
		ve(OptString("✓", nil)),
		ve("✓", nil),
	},
	{
		`OptString(nil)`,
		ve(OptString(nil, nil)),
		ve("", nil),
	},
	{
		"Int64s([v1, v2])",
		ve(Int64s([]interface{}{[]byte("4"), []byte("5")}, nil)),
		ve([]int64{4, 5}, nil),
	},
	{
		"Int64s(nil)",
		ve(Int64s(nil, nil)),
		ve([]int64(nil), ErrNil),
	},
	{
		`Int64ToBytesMapping([]interface{}{4, "four"})`,
		ve(Int64ToBytesMapping([]interface{}{int64(4), []byte("four")}, nil)),
		ve(map[int64][]byte{4: []byte("four")}, nil),
	},
}

func TestReply(t *testing.T) {
	for _, rt := range replyTests {
		if rt.actual.err != rt.expected.err {
			t.Errorf("%s returned err %v, want %v", rt.name, rt.actual.err, rt.expected.err)
			continue
		}
		if !reflect.DeepEqual(rt.actual.v, rt.expected.v) {
			t.Errorf("%s=%+v, want %+v", rt.name, rt.actual.v, rt.expected.v)
		}
	}
}
<<<<<<< 94d515495f2338f810b0c351e9aa6061ac4b655b
=======

func ExampleBool() {
	c, err := dial()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Do("SET", "foo", 1)
	exists, err := Bool(c.Do("EXISTS", "foo"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", exists)
	// Output:
	// true
}

// TODO:
// Add more examples:
// ExampleError, ExampleBytes, ExampleOptBytes, ExampleInt64,
// ExampleBools, ExampleStrings, ExampleInt64ToBytesMapping
// part of https://github.com/zero-os/0-Disk/issues/543

// dial creates an inmemory ledisdb server, and dials it.
func dial() (redis.Conn, error) {
	server := ledisdb.NewServer()
	return Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
}
>>>>>>> add+use new storage primitives (except copy)
