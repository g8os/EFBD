package ardb

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

// Error is a helper that allows you to extract just the error from a reply.
func Error(_ interface{}, err error) error {
	return err
}

// Bytes is a helper that converts a command reply to a slice of bytes. If err
// is not equal to nil, then Bytes returns nil,
func Bytes(reply interface{}, err error) ([]byte, error) {
	return redis.Bytes(reply, err)
}

// OptBytes tries to interpret the given reply as a byte slice,
// however if no reply is returned, this function will return a nil slice,
// rather than an error.
func OptBytes(reply interface{}, err error) ([]byte, error) {
	if reply == nil {
		return nil, err
	}
	return redis.Bytes(reply, err)
}

// Bool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err.
func Bool(reply interface{}, err error) (bool, error) {
	return redis.Bool(reply, err)
}

// OptBool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err.
// If reply is `nil`, than it returns false, err as well.
func OptBool(reply interface{}, err error) (bool, error) {
	if reply == nil {
		return false, err
	}
	return redis.Bool(reply, err)
}

// Int is a helper that converts a command reply to signed 32/64 bit integer. If err is
// not equal to nil, then it returns 0, err.
func Int(reply interface{}, err error) (int, error) {
	return redis.Int(reply, err)
}

// OptInt is a helper that converts a command reply to signed 32/64 bit integer. If err is
// not equal to nil, then it returns 0, err.
// If reply is `nil`, than it returns 0, err as well.
func OptInt(reply interface{}, err error) (int, error) {
	if reply == nil {
		return 0, err
	}
	return redis.Int(reply, err)
}

// Int64 is a helper that converts a command reply to signed 64 bit integer. If err is
// not equal to nil, then it returns 0, err.
func Int64(reply interface{}, err error) (int64, error) {
	return redis.Int64(reply, err)
}

// OptInt64 is a helper that converts a command reply to signed 64 bit integer. If err is
// not equal to nil, then it returns 0, err.
// If reply is `nil`, than it returns 0, err as well.
func OptInt64(reply interface{}, err error) (int64, error) {
	if reply == nil {
		return 0, err
	}
	return redis.Int64(reply, err)
}

// Uint64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then it returns 0, err.
func Uint64(reply interface{}, err error) (uint64, error) {
	return redis.Uint64(reply, err)
}

// OptUint64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then it returns 0, err.
// If reply is `nil`, than it returns 0, err as well.
func OptUint64(reply interface{}, err error) (uint64, error) {
	if reply == nil {
		return 0, err
	}
	return redis.Uint64(reply, err)
}

// String is a helper that converts a command reply to a string. If err is
// not equal to nil, then it returns "", err.
func String(reply interface{}, err error) (string, error) {
	return redis.String(reply, err)
}

// OptString is a helper that converts a command reply to a string. If err is
// not equal to nil, then it returns "", err.
// If reply is `nil`, than it returns "", err as well.
func OptString(reply interface{}, err error) (string, error) {
	if reply == nil {
		return "", err
	}
	return redis.String(reply, err)
}

// Int64s is a helper that converts an array command reply to a []int64.
// If err is not equal to nil, then Int64s returns the error.
// Int64s returns an error if an array item is not an integer.
func Int64s(reply interface{}, err error) ([]int64, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	var ints []int64
	if err := redis.ScanSlice(values, &ints); err != nil {
		return nil, err
	}
	if len(ints) == 0 {
		return nil, ErrNil
	}

	return ints, nil
}

// Bools is a helper that converts an array command reply to a []bool.
// If err is not equal to nil, then Bools returns the error.
// Bools returns an error if an array item is not an bool.
func Bools(reply interface{}, err error) ([]bool, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	var bools []bool
	if err := redis.ScanSlice(values, &bools); err != nil {
		return nil, err
	}
	if len(bools) == 0 {
		return nil, ErrNil
	}

	return bools, nil
}

// Strings is a helper that converts an array command reply to a []string.
// If err is not equal to nil, then Strings returns the error.
// Strings returns an error if an array item is not an string.
func Strings(reply interface{}, err error) ([]string, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	var strings []string
	if err := redis.ScanSlice(values, &strings); err != nil {
		return nil, err
	}
	if len(strings) == 0 {
		return nil, ErrNil
	}

	return strings, nil
}

// Int64ToBytesMapping is a helper that converts an array command reply to a map[int64][]byte.
// If err is not equal to nil, then this function returns the error.
// If the given reply can also not be transformed into a `map[int64][]byte` this function returns an error as well.
func Int64ToBytesMapping(reply interface{}, err error) (map[int64][]byte, error) {
	values, err := redis.Values(reply, err)
	if err != nil {
		return nil, err
	}

	n := len(values)
	if n%2 != 0 {
		return nil, errors.New("Int64ToBytesMapping expects even number of values result")
	}

	m := make(map[int64][]byte, n)
	for i := 0; i < n; i += 2 {
		key, err := redis.Int64(values[i], nil)
		if err != nil {
			return nil, err
		}
		value, err := redis.Bytes(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		m[key] = value
	}
	return m, nil
}

var (
	// ErrNil indicates that a reply value is nil.
	ErrNil = redis.ErrNil
)
