package itsyouonline

// Permission defines itsyouonline permission for an org & namespace
type Permission struct {
	Read   bool
	Write  bool
	Delete bool
	Admin  bool
}

func (p Permission) perms() []string {
	var perms []string
	if p.Read {
		perms = append(perms, "read")
	}
	if p.Write {
		perms = append(perms, "write")
	}
	if p.Delete {
		perms = append(perms, "delete")
	}
	if p.Admin {
		perms = append(perms, "admin")
	}
	return perms
}

func (p Permission) Scopes(org, namespace string) []string {
	var scopes []string
	scopePrefix := "user:memberof:" + org + "." + namespace + "."
	for _, p := range p.perms() {
		if p == "admin" {
			scopes = append(scopes, scopePrefix[:len(scopePrefix)-1])
		} else {
			scopes = append(scopes, scopePrefix+p)
		}
	}
	return scopes
}
