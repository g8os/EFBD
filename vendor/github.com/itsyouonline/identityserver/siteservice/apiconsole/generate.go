package apiconsole

/*
OK, mulesofts api-console is nice to render the raml files.
I don't want to run a node server or run bower on the server nor do I want to check in all uncleaned bower packages.
Only a clean distribution of the api-console. Luckily mulesoft has just that in their git repo.

Using github svn access is a trick to download that subfolder, next, package it using go-bindata to include the files in the binary.

*/

//go:generate svn export --force https://github.com/mulesoft/api-console/branches/master/dist
//go:generate go-bindata -pkg apiconsole -prefix dist -o apiconsole.go -ignore "index.html|favicon.ico|examples" dist/...
