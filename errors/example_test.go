package errors_test

import (
	"fmt"

	"github.com/zero-os/0-Disk/errors"
)

func ExampleNew() {
	err := errors.New("an error message")

	fmt.Print(err.Error())
	// Output: an error message
}

func ExampleNewf() {
	err := errors.Newf("this is %d error %s with %d arguments", 1, "message", 3)

	fmt.Print(err.Error())
	// Output: this is 1 error message with 3 arguments
}

func ExampleWrap() {
	err := errors.New("this is the original error message")

	err = errors.Wrap(err, "this is an annotation wrapped around the error")

	fmt.Print(err.Error())
	// Output:
	// this is an annotation wrapped around the error: this is the original error message
}

func ExampleWrapf() {
	err := errors.New("this is the original error message")

	err = errors.Wrapf(err, "this is a %s annotation with %d args ", "formatted", 2)

	fmt.Print(err.Error())
	// Output:
	// this is a formatted annotation with 2 args : this is the original error message
}

func ExampleCause() {
	err := errors.New("this is the cause of the error")
	err = errors.Wrap(err, "this is an annotation wrapped around the error")

	cause := errors.Cause(err)

	fmt.Print(cause.Error())
	// Output: this is the cause of the error
}

func ExampleWrapError() {
	errCause := errors.New("this is the cause error")
	errAnnot := errors.New("this is the annotation error")

	err := errors.WrapError(errCause, errAnnot)

	fmt.Print(err.Error())
	// Output: this is the annotation error: this is the cause error
}

func ExampleCause_multipleWraps() {
	err := errors.New("cause of the error")
	err = errors.Wrap(err, "annotation 1")
	err = errors.Wrap(err, "annotation 2")
	err = errors.Wrap(err, "annotation 3")

	cause := errors.Cause(err)

	fmt.Println(err.Error())
	fmt.Println(cause.Error())
	// Output:
	// annotation 3: annotation 2: annotation 1: cause of the error
	// cause of the error
}

func Example_errorSlice() {
	errs := errors.NewErrorSlice()

	fmt.Println(errs.Len())

	if errs.AsError() == nil {
		fmt.Println("no errors")
	} else {
		fmt.Println("there shouldn't be any errors in here yet")
	}

	errs.Add(errors.New("an error"))

	fmt.Println(errs.Len())
	fmt.Println(errs.Error())

	errs.Add(errors.New("another error"))

	fmt.Println(errs.Len())
	fmt.Println(errs.Error())

	// Output:
	// 0
	// no errors
	// 1
	// an error;
	// 2
	// an error;another error;
}
