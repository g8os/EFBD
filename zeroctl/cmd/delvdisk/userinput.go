package delvdisk

import (
	"errors"
	"fmt"

	valid "github.com/asaskevich/govalidator"
)

// userInput is parsed from the positional arguments
type userInput struct {
	VdiskID string
	URL     string
}

// parse user input from the positional arguments
// given by the user when calling this command line utility
func parseUserInput(args []string) (*userInput, error) {
	argn := len(args)

	if argn < 2 {
		return nil, errors.New("not enough arguments")
	}
	if argn > 2 {
		return nil, errors.New("too many arguments")
	}

	input := new(userInput)
	input.VdiskID = args[0]
	input.URL = args[1]

	if !valid.IsDialString(input.URL) {
		return nil, fmt.Errorf(
			"%q is not a valid dial string", input.URL)
	}

	return input, nil
}
