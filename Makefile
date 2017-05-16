OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

PACKAGE = github.com/g8os/blockstor
COMMIT_HASH = $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE = $(shell date +%FT%T%z)

PACKAGES = $(shell go list ./... | grep -v vendor)

ldflags = -extldflags "-static"
ldflagsg8stor = -X $(PACKAGE)/g8stor/cmd.CommitHash=$(COMMIT_HASH) -X $(PACKAGE)/g8stor/cmd.BuildDate=$(BUILD_DATE)

all: nbdserver tlogserver g8stor

g8stor: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagsg8stor)' -o $(OUTPUT)/$@ ./g8stor
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags) $(ldflagsg8stor)' -o $(OUTPUT)/$@ ./g8stor
endif

nbdserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(OUTPUT)/$@ ./nbdserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/$@ ./nbdserver
endif

tlogserver: $(OUTPUT)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/$@ ./tlog/tlogserver

test: testgo testcgo
	./scripts/codegeneration.sh

testgo:
	go test -timeout 5m $(PACKAGES)

testcgo:
	GODEBUG=cgocheck=0 go test -timeout 5m -tags 'isal' $(PACKAGES)

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) nbdserver tlogserver g8stor test testgo testcgo
