package zerolog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestLogLevelSwitch(t *testing.T) {
	// stdout
	err := Log(LevelStdout, "Hello world")
	assertNoError(t, err)

	// stderr
	err = Log(LevelStderr, "Hello world")
	assertNoError(t, err)

	// json
	var ts testStruct
	err = Log(LevelJSON, ts)
	assertNoError(t, err)

	// invalid
	err = Log(255, "Hello world")
	assertError(t, err)
	assertEqual(t, ErrLevelNotValid.Error(), err.Error())

	// nil message
	err = Log(1, nil)
	assertError(t, err)
	assertEqual(t, ErrNilMessage.Error(), err.Error())

	// empty message
	err = Log(1, "")
	assertError(t, err)
	assertEqual(t, ErrNilMessage.Error(), err.Error())
}

func TestStringInput(t *testing.T) {
	// check valid strings
	//normal string
	err := Log(LevelStdout, "Hello\nworld")
	assertNoError(t, err)

	//string alias
	var sa stringAlias
	sa = "Hello world"
	err = Log(LevelStdout, sa)
	assertNoError(t, err)

	//implements stringer
	st := stringer{
		s: "lorem ipsum",
	}
	err = Log(LevelStdout, st)
	assertNoError(t, err)

	//implements TextMarshaler
	tm := textMarchal{
		"dolor sit amet",
	}
	err = Log(LevelStdout, tm)
	assertNoError(t, err)

	// check invalid strings
	//empty struct
	err = Log(LevelStdout, struct{}{})
	assertError(t, err)
	assertEqual(t, "could not turn message into string", err.Error())

	//alias
	var ia intAlias
	ia = 1
	err = Log(LevelStdout, ia)
	assertError(t, err)

	// TextMarshaler error
	var tme textMarchalError
	err = Log(LevelStdout, tme)
	assertError(t, err)
}

// test types for TestStringInput
type stringAlias string
type intAlias int

// test type that implements fmt.Stringer
type stringer struct {
	s string
}

func (s stringer) String() string {
	return s.s
}

// test type that implements encoding.TextMarshaler
type textMarchal struct {
	s string
}

func (tm textMarchal) MarshalText() ([]byte, error) {
	return []byte(tm.s), nil
}

// test type that implements encoding.TextMarshaler
// returns an error
type textMarchalError struct {
}

func (tm textMarchalError) MarshalText() ([]byte, error) {
	return nil, fmt.Errorf("An expected error")
}
func TestJSONInput(t *testing.T) {

	// marshal test structure and check output
	tstruct := testStruct{
		TestField:      "Hello world",
		OtherTestfield: 1,
	}
	tstructExpected := "20::{\"TestField\":\"Hello world\",\"OtherTestfield\":1}\n"

	// check no error if logged
	err := Log(LevelJSON, tstruct)
	if !assertNoError(t, err) {
		return
	}

	// check output is as expected
	jsonStr, err := json.Marshal(tstruct)
	if !assertNoError(t, err) {
		return
	}

	var tw testWriter
	printLog(&tw, LevelJSON, string(jsonStr))

	assertEqual(t, tstructExpected, tw.Val)

	// write a value json can't marshal
	err = Log(LevelJSON, math.Inf(1))
	assertError(t, err)

}

func TestFormatLog(t *testing.T) {
	input1 := "stdout single line test"
	expectResult1 := "1::stdout single line test\n"
	var tw testWriter
	printLog(&tw, LevelStdout, input1)

	assertEqual(t, expectResult1, tw.Val)

	input2 := "stderr\nmultiline test"
	expectResult2 := "2:::\nstderr\nmultiline test\n:::\n"
	printLog(&tw, LevelStderr, input2)

	assertEqual(t, expectResult2, tw.Val)
}

func TestMultiline(t *testing.T) {
	str1 := `
This 
is a
multilined string	
`
	str2 := "This one\nis too"
	str3 := "this one is not"

	assertTrue(t, isMultiline(str1))
	assertTrue(t, isMultiline(str2))
	assertFalse(t, isMultiline(str3))
}

type testStruct struct {
	TestField      string
	OtherTestfield int
}

type testWriter struct {
	Val string
}

func (tw *testWriter) Write(p []byte) (int, error) {
	tw.Val = string(p)
	return len(p), nil
}

// test assertion
func assertTrue(t *testing.T, val bool) bool {
	if !val {
		t.Error("Expected True, got False")
		return false
	}
	return true
}

func assertFalse(t *testing.T, val bool) bool {
	if val {
		t.Error("Expected False, got True")
		return false
	}
	return true
}
func assertNoError(t *testing.T, err error) bool {
	if err != nil {
		t.Errorf("Did not expect error, got: %s", err)
		return false
	}

	return true
}

func assertError(t *testing.T, err error) bool {
	if err == nil {
		t.Error("Expected an error, got nil")
		return false
	}

	return true
}

func assertEqual(t *testing.T, expected, actual interface{}) bool {
	if !ObjectsAreEqual(expected, actual) {
		t.Errorf("Values were not equal\nExpected: %v\nActual: %v\n", expected, actual)
		return false
	}

	return true
}

// equal check
// source: github.com/stretchr/testify/assert/assertions.go
func ObjectsAreEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	if exp, ok := expected.([]byte); ok {
		act, ok := actual.([]byte)
		if !ok {
			return false
		} else if exp == nil || act == nil {
			return exp == nil && act == nil
		}
		return bytes.Equal(exp, act)
	}

	return reflect.DeepEqual(expected, actual)
}
