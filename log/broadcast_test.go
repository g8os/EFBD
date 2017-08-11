package log

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageSubjectMarshal(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		Input  MessageSubject
		Output string
	}{
		{SubjectETCD, subjectETCDStr},
		{SubjectStorage, subjectStorageStr},
		{SubjectTlog, subjectTlogStr},
	}

	for _, testCase := range testCases {
		bytes, err := json.Marshal(testCase.Input)
		if !assert.NoError(err, "error while marshalling %v", testCase.Input) {
			continue
		}

		assert.Equal(string(bytes), `"`+testCase.Output+`"`)
	}
}
