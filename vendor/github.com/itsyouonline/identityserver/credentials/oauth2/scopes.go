package oauth2

import "strings"

//SplitScopeString takes a comma seperated string representation of scopes and returns it as a slice
func SplitScopeString(scopestring string) (scopeList []string) {
	scopeList = []string{}
	for _, value := range strings.Split(scopestring, ",") {
		scope := strings.TrimSpace(value)
		if scope != "" {
			scopeList = append(scopeList, scope)
		}
	}
	return
}

// CheckScopes checks whether one of the possibleScopes is in the authorized scopes list
func CheckScopes(possibleScopes []string, authorizedScopes []string) bool {
	if len(possibleScopes) == 0 {
		return true
	}

	for _, allowed := range possibleScopes {
		for _, scope := range authorizedScopes {
			if scope == allowed {
				return true
			}
		}
	}
	return false
}
