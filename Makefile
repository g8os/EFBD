OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

TIMEOUT ?= 2m

PACKAGE = github.com/zero-os/0-Disk
COMMIT_HASH = $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE = $(shell date +%FT%T%z)

PACKAGES = $(shell go list ./... | grep -v vendor)
RACE_PACKAGES = $(shell go list ./... | grep -v vendor | grep -E 'nbd|config' | grep -v 'gonbdserver')

ldflags = -extldflags "-static" -s -w
ldflagszeroctl = -X $(PACKAGE)/zeroctl/cmd.CommitHash=$(COMMIT_HASH) -X $(PACKAGE)/zeroctl/cmd.BuildDate=$(BUILD_DATE) -s -w

all: nbdserver tlogserver zeroctl

zeroctl: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagszeroctl)' -o $(OUTPUT)/$@ ./zeroctl
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags) $(ldflagszeroctl)' -o $(OUTPUT)/$@ ./zeroctl
endif

nbdserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(OUTPUT)/$@ ./nbd/nbdserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/$@ ./nbd/nbdserver
endif

tlogserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(OUTPUT)/$@ ./tlog/tlogserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags)' -o $(OUTPUT)/$@ ./tlog/tlogserver
endif

test: testgo testrace testcgo testcodegen

testgo:
	go test -timeout $(TIMEOUT) $(PACKAGES)

testrace:
	go test -race -timeout $(TIMEOUT) $(RACE_PACKAGES)

testrace_tlog_client:
	go test -race -timeout $(TIMEOUT) github.com/zero-os/0-Disk/tlog/tlogclient/...

testrace_tlog_server:
	go test -race -timeout $(TIMEOUT) github.com/zero-os/0-Disk/tlog/tlogserver/...

testrace_gonbdserver:
	go test -race -timeout $(TIMEOUT) github.com/zero-os/0-Disk/nbd/gonbdserver/nbd

testcodegen:
	./scripts/codegeneration.sh

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) nbdserver tlogserver zeroctl test testgo testrace testcgo testcodegen
