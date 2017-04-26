package gridapi

// grid api client
//go:generate go-raml client --ramlfile gridspec/api.raml --dir gridapiclient --package gridapiclient

// grid api stub
//go:generate go-raml server --ramlfile gridspec/api.raml --dir gridapistub --package gridapistub --no-apidocs
