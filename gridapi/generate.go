package gridapi

//go:generate curl -O https://raw.githubusercontent.com/g8os/grid/0.2.0/raml/types.raml
//go:generate curl -O https://raw.githubusercontent.com/g8os/grid/0.2.0/raml/api.raml

// grid api client
//go:generate go-raml client --ramlfile api.raml --dir gridapiclient --package gridapiclient

// grid api stub
//go:generate go-raml server --ramlfile api.raml --dir gridapistub --package gridapistub
