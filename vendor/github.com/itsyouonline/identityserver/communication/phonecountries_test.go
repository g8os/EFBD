package communication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRussianMobileNumber(t *testing.T) {
	phonenumbers := []string{
		"+32492440022",
		"+72891254882",
		"79412651619",
		"+797989815658",
		"+7856411156",
	}
	assert.False(t, IsRussianMobileNumber(phonenumbers[0]))
	assert.True(t, IsRussianMobileNumber(phonenumbers[1]))
	assert.False(t, IsRussianMobileNumber(phonenumbers[2]))
	assert.False(t, IsRussianMobileNumber(phonenumbers[3]))
	assert.False(t, IsRussianMobileNumber(phonenumbers[4]))
}
