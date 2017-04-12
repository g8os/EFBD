package redisstub

import (
	"bytes"
	"fmt"
	"strconv"
)

func asByteSlice(arg interface{}) []byte {
	switch arg := arg.(type) {
	case []byte:
		return arg

	case string:
		return []byte(arg)

	case int64:
		return []byte(strconv.FormatInt(arg, 10))
	case int:
		return []byte(strconv.Itoa(arg))
	case int32:
		return []byte(strconv.FormatInt(int64(arg), 10))
	case int8:
		return []byte(strconv.FormatInt(int64(arg), 10))
	case int16:
		return []byte(strconv.FormatInt(int64(arg), 10))

	case uint:
		return []byte(strconv.FormatUint(uint64(arg), 10))
	case uint8:
		return []byte(strconv.FormatUint(uint64(arg), 10))
	case uint16:
		return []byte(strconv.FormatUint(uint64(arg), 10))
	case uint32:
		return []byte(strconv.FormatUint(uint64(arg), 10))
	case uint64:
		return []byte(strconv.FormatUint(arg, 10))

	case float32:
		return []byte(strconv.FormatFloat(float64(arg), 'E', -1, 64))
	case float64:
		return []byte(strconv.FormatFloat(arg, 'E', -1, 64))

	case bool:
		if arg {
			return []byte("1")
		}
		return []byte("0")

	case nil:
		return []byte("")

	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return buf.Bytes()
	}
}

func asInt64(arg interface{}) (i int64, err error) {
	switch arg := arg.(type) {
	case int64:
		i = arg

	case int:
		i = int64(arg)
	case int32:
		i = int64(arg)
	case int8:
		i = int64(arg)
	case int16:
		i = int64(arg)

	case uint32:
		i = int64(arg)
	case uint8:
		i = int64(arg)
	case uint16:
		i = int64(arg)

	default:
		err = fmt.Errorf(
			"%v (%T) cannot be casted into an int64 value",
			arg, arg)
	}

	return
}
