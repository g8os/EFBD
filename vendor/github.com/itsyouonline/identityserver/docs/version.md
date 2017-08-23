## Get the currently deployed version:

In order to get the currently deployed version from the (staging) server, a `GET` request
should be send. The reply form the server will contain a body with the following json:
`{"version": CURRENT_VERSION}`. `CURRENT_VERSION` will match the output form the command
`git describe` when executed in the repository root of the server.
