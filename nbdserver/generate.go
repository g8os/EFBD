package main

// grid api client
//go:generate go-raml client --ramlfile specs/gridapi/api.raml --dir clients/gridapi --package gridapi

// grid api stub
//go:generate go-raml server --ramlfile specs/gridapi/api.raml --dir stubs/gridapi --package gridapi
