package goraml

import (
	"fmt"
	"strconv"
)

func MultipleOf(num interface{}, value string) error {
	var numFloat float64

	switch v := num.(type) {
	case int:
		numFloat = float64(v)
	case float64:
		numFloat = v
	default:
		return fmt.Errorf("%v can't be used with multipleOf", v)
	}

	valueFloat, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}

	res := numFloat / valueFloat
	if (res / float64(int(res))) != 1.0 {
		return fmt.Errorf("%v is not multipleOf %v", num, value)
	}

	return nil
}
