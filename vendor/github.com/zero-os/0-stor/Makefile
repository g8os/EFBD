OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

TIMEOUT ?= 10m

PACKAGE = github.com/zero-os/0-stor
COMMIT_HASH = $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE = $(shell date +%FT%T%z)

SERVER_PACKAGES = $(shell go list ./server/...)
CLIENT_PACKAGES = $(shell go list ./client/...)

ldflags = -extldflags "-static" -s -w

all: server cli

cli: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/zerostorcli ./client/cmd/cli
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/zerostorcli ./client/cmd/cli
endif

server: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(OUTPUT)/zerostorserver ./server
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/zerostorserver ./server
endif

test: testserver testclient

testrace: testserverrace testclientrace

testserver:
	go test -timeout $(TIMEOUT) $(SERVER_PACKAGES)

testclient:
	go test -timeout $(TIMEOUT) $(CLIENT_PACKAGES)

testserverrace:
	go test -race -timeout $(TIMEOUT) $(SERVER_PACKAGES)

testclientrace:
	go test -race -timeout $(TIMEOUT) $(CLIENT_PACKAGES)

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) zerostorcli 0storserver test testserver testclient
