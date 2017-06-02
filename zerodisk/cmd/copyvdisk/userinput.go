package copyvdisk

import (
	"errors"
	"fmt"

	valid "github.com/asaskevich/govalidator"
)

// userInput is parsed from the partial positional arguments
type userInput struct {
	VdiskID string
	URL     string
}

// userInputPair is parsed from the total positional arguments
type userInputPair struct {
	Source userInput
	Target userInput
}

// parse user input from the positional arguments
// given by the user when calling this command line utility
func parseUserInput(args []string) (input *userInputPair, err error) {
	argn := len(args)

	if argn < 3 {
		err = errors.New("not enough arguments")
		return
	}
	if argn > 4 {
		err = errors.New("too many arguments")
		return
	}

	input = new(userInputPair)

	// store required args
	input.Source.VdiskID = args[0]
	input.Target.VdiskID = args[1]
	input.Source.URL = args[2]

	if !valid.IsDialString(input.Source.URL) {
		return nil, fmt.Errorf(
			"%q is not a valid dial string", input.Source.URL)
	}

	// store optional args
	if argn == 4 {
		input.Target.URL = args[3]
		if !valid.IsDialString(input.Target.URL) {
			return nil, fmt.Errorf(
				"%q is not a valid dial string", input.Target.URL)
		}
	} else {
		// if no explicit target_url is given
		// it is assumed that the same url
		// for both source and target is to be used
		input.Target.URL = args[2]
	}

	return
}
