package rest

type EnumCheckStatusStatus string

const (
	EnumCheckStatusStatusok        EnumCheckStatusStatus = "ok"
	EnumCheckStatusStatuscorrupted EnumCheckStatusStatus = "corrupted"
	EnumCheckStatusStatusmissing   EnumCheckStatusStatus = "missing"
)
