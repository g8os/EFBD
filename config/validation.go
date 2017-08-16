package config

import (
	"strings"

	valid "github.com/asaskevich/govalidator"
)

// IsServiceAddress checks if a string is
// a valid service address,
// which means it's either a unix address or a dial string.
func IsServiceAddress(str string) bool {
	// for add
	if strings.HasPrefix(str, unixPrefix) {
		// according to http://godoc.org/net#Dial
		// >> For Unix networks, the address must be a [valid] file system path.
		ok, pt := valid.IsFilePath(str[unixFileStart:])
		return ok && pt == valid.Unix
	}

	if strings.HasPrefix(str, httpPrefix) || strings.HasPrefix(str, httpsPrefix) {
		return valid.IsURL(str)
	}

	return valid.IsDialString(str)
}

const (
	unixPrefix    = "unix://"
	httpPrefix    = "http://"
	httpsPrefix   = "https://"
	unixFileStart = len(unixPrefix) - 1
)

func init() {
	valid.TagMap["serviceaddress"] = IsServiceAddress
}
