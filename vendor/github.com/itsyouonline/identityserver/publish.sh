#!/usr/bin/env bash
set -e

VERSION="$(git describe)"

echo "Building version $VERSION"

docker build -t itsyouonlinebuilder .
docker run --rm -v "$PWD":/go/src/github.com/itsyouonline/identityserver --entrypoint sh itsyouonlinebuilder -c "go generate && go build -ldflags '-s -X main.version=$VERSION' -v -o dist/identityserver"
docker build -t itsyouonline/identityserver:"$VERSION" -f DockerfileMinimal .

docker push itsyouonline/identityserver:"$VERSION"
