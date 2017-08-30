package zerolog

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"runtime"
	"testing"
)

func TestLogLevelSwitch(t *testing.T) {
	// stdout
	err := Log(LevelStdout, "Hello world")
	assertNilError(t, err)

	// stderr
	err = Log(LevelStderr, "Hello world")
	assertNilError(t, err)

	// json
	var ts testStruct
	err = Log(LevelJSON, ts)
	assertNilError(t, err)

	// invalid
	err = Log(255, "Hello world")
	assertError(t, err)
	assertEqual(t, ErrLevelNotValid, err)

	// nil message
	err = Log(1, nil)
	assertError(t, err)
	assertEqual(t, ErrNilMessage, err)

	// empty message
	err = Log(1, "")
	assertError(t, err)
	assertEqual(t, ErrNilMessage, err)

	// test nil messages
	err = Log(LevelStderr, nil)
	assertError(t, err)
	err = Log(LevelStdout, nil)
	assertError(t, err)
	err = Log(LevelJSON, nil)
	assertError(t, err)
	err = Log(LevelStatistics, nil)
	assertError(t, err)
}

func TestStringInput(t *testing.T) {
	// check valid strings
	//normal string
	err := Log(LevelStdout, "Hello\nworld")
	assertNilError(t, err)

	//string alias
	var sa stringAlias
	sa = "Hello world"
	err = Log(LevelStdout, sa)
	assertNilError(t, err)

	//implements stringer
	st := stringer{
		s: "lorem ipsum",
	}
	err = Log(LevelStdout, st)
	assertNilError(t, err)

	//implements TextMarshaler
	tm := textMarshal{
		"dolor sit amet",
	}
	err = Log(LevelStdout, tm)
	assertNilError(t, err)

	// check invalid strings
	//empty struct
	err = Log(LevelStdout, struct{}{})
	if assertError(t, err) {
		assertEqual(t, ErrInvalidMessage, err)
	}

	//alias
	var ia intAlias
	ia = 1
	err = Log(LevelStdout, ia)
	assertError(t, err)

	// TextMarshaler error
	var tme textMarchalError
	err = Log(LevelStdout, tme)
	assertError(t, err)

	//empty string in msgString
	_, err = msgString("")
	if assertError(t, err) {
		assertEqual(t, ErrNilMessage, err)
	}
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
type textMarshal struct {
	s string
}

func (tm textMarshal) MarshalText() ([]byte, error) {
	return []byte(tm.s), nil
}

// test type that implements encoding.TextMarshaler
// returns an error
type textMarchalError struct {
}

func (tm textMarchalError) MarshalText() ([]byte, error) {
	return nil, fmt.Errorf("An expected error")
}

func TestStatsInput(t *testing.T) {
	valFullStatMsg := MsgStatistics{
		Key:       "somekey",
		Value:     123.456,
		Operation: AggregationAverages,
		Tags: map[string]interface{}{
			"foo": "bar",
		},
	}

	// test message formatting
	str, err := msgStatistics(valFullStatMsg)
	assertNilError(t, err)
	assertEqual(t, "somekey:123.456|A|foo=bar", str)

	// test full log output
	var tw testWriter
	printLog(&tw, LevelStatistics, str)
	assertEqual(t, "10::somekey:123.456|A|foo=bar\n", tw.Val)

	// test message formatting with a floating value,
	// which ensures we are not rounding up to 6,
	// and instead using as much precision as is needed to preserve the value
	valFullStatMsg.Value = 1.1234000001
	str, err = msgStatistics(valFullStatMsg)
	assertNilError(t, err)
	assertEqual(t, "somekey:1.1234000001|A|foo=bar", str)

	// test invalid  message
	_, err = msgStatistics("")
	if assertError(t, err) {
		assertEqual(t, ErrInvalidMessage, err)
	}

	invalKey := MsgStatistics{
		Key:       "",
		Value:     123.456,
		Operation: AggregationDifferentiates,
		Tags: map[string]interface{}{
			"foo": "bar",
		},
	}
	_, err = msgStatistics(invalKey)
	if assertError(t, err) {
		assertEqual(t, ErrNilStatisticsKey, err)
	}

	invalOperation := MsgStatistics{
		Key:       "someKey",
		Value:     123.456,
		Operation: "B",
		Tags: map[string]interface{}{
			"foo": "bar",
		},
	}
	_, err = msgStatistics(invalOperation)
	if assertError(t, err) {
		assertEqual(t, ErrInvalidAggregationType, err)
	}

	// test logging valid Stats messages
	err = Log(LevelStatistics, valFullStatMsg)
	assertNilError(t, err)

	// test logging invalid Stats messages
	err = Log(LevelStatistics, invalKey)
	assertError(t, err)
}

func TestJSONInput(t *testing.T) {
	// marshal test structure and check output
	tstruct := testStruct{
		TestField:      "Hello world",
		OtherTestfield: 1,
	}

	// check no error is logged
	err := Log(LevelJSON, tstruct)
	assertNilError(t, err)

	// check output is as expected
	jsonStr, err := json.Marshal(tstruct)
	assertNilError(t, err)

	var tw testWriter
	printLog(&tw, LevelJSON, string(jsonStr))

	assertEqual(t, "20::{\"TestField\":\"Hello world\",\"OtherTestfield\":1}\n", tw.Val)

	// write a value json can't marshal
	err = Log(LevelJSON, math.Inf(1))
	assertError(t, err)

	// check formatted string output
	str, err := msgJSON(tstruct)
	assertEqual(t, "{\"TestField\":\"Hello world\",\"OtherTestfield\":1}", str)
}

func TestPrintLog(t *testing.T) {
	input1 := "stdout single line test"
	var tw testWriter
	printLog(&tw, LevelStdout, input1)
	assertEqual(t, "1::stdout single line test\n", tw.Val)

	input2 := "stderr\nmultiline test"
	printLog(&tw, LevelStderr, input2)
	assertEqual(t, "2:::\nstderr\nmultiline test\n:::\n", tw.Val)

}

func TestMultiline(t *testing.T) {
	str1 := `
This 
is a
multilined string	
`
	str2 := "This one\nis too"
	str3 := "this one is not"

	assertEqual(t, true, isMultiline(str1))
	assertEqual(t, true, isMultiline(str2))
	assertEqual(t, false, isMultiline(str3))
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

func TestAggregationType(t *testing.T) {
	a := AggregationAverages
	d := AggregationDifferentiates
	var z AggregationType
	z = "z"

	assertNilError(t, a.Validate())
	assertNilError(t, d.Validate())
	assertError(t, z.Validate())
}

func TestMetricTags(t *testing.T) {
	emptyTags := MetricTags{}
	assertEqual(t, "", emptyTags.String())

	mockTagsString := MetricTags{
		"foo":   "bar",
		"hello": "world",
	}
	assertNilError(t, checkMetricsResult(mockTagsString))

	mockTagsByteSlice := MetricTags{
		"foo":   []byte("bar"),
		"hello": []byte("world"),
	}
	assertNilError(t, checkMetricsResult(mockTagsByteSlice))

	barS := metricResultStringer{s: "bar"}
	worldS := metricResultStringer{s: "world"}
	mockTagsStringer := MetricTags{
		"foo":   barS,
		"hello": worldS,
	}
	assertNilError(t, checkMetricsResult(mockTagsStringer))

	barTM := metricResultTextMarshall{s: "bar"}
	worldTM := metricResultTextMarshall{s: "world"}
	mockTagsTextMarshal := MetricTags{
		"foo":   barTM,
		"hello": worldTM,
	}
	assertNilError(t, checkMetricsResult(mockTagsTextMarshal))

	// test last resort
	barAnon := metricResultAnon{s: "bar"}
	worldAnon := metricResultAnon{s: "world"}
	mockTagsAnon := MetricTags{
		"foo":   barAnon,
		"hello": worldAnon,
	}
	assertNilError(t, checkMetricsResult(mockTagsAnon))
}

func checkMetricsResult(mt MetricTags) error {
	// order is not guaranteed
	switch mt.String() {
	case "foo=bar,hello=world", "hello=world,foo=bar":
		return nil
	// for last resort check
	case "foo={bar},hello={world}", "hello={world},foo={bar}":
		return nil
	default:
		return fmt.Errorf("unexpected MetricTags result: %s", mt)
	}
}

type metricResultStringer struct {
	s string
}

func (mrs metricResultStringer) String() string {
	return mrs.s
}

type metricResultTextMarshall struct {
	s string
}

func (mrtm metricResultTextMarshall) MarshalText() ([]byte, error) {
	return []byte(mrtm.s), nil
}

type metricResultAnon struct {
	s string
}

// local assert utilities to ease the testing

func assertEqual(t *testing.T, expected, value interface{}) bool {
	if expected != value {
		file, line := callerInfo()
		t.Errorf("unexpected value at %s@%d: expected '%v', but received '%v'",
			file, line, expected, value)
		return false
	}

	return true
}

func assertNilError(t *testing.T, err error) bool {
	if err != nil {
		file, line := callerInfo()
		t.Errorf("unexpected error at %s@%d: %s", file, line, err)
		return false
	}

	return true
}

func assertError(t *testing.T, err error) bool {
	if err == nil {
		file, line := callerInfo()
		t.Errorf("expected error at %s@%d but received nil", file, line)
		return false
	}

	return true
}

func callerInfo() (file string, line int) {
	_, file, line, _ = runtime.Caller(2)
	file = path.Base(file)
	return
}
