OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

TIMEOUT ?= 5m

PACKAGE = github.com/zero-os/0-Disk
COMMIT_HASH = $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE = $(shell date +%FT%T%z)

PACKAGES = $(shell go list ./... | grep -v vendor)
RACE_PACKAGES = $(shell go list ./... | grep -v vendor | grep -E 'nbd|config' | grep -v 'gonbdserver')

ldflags = -extldflags "-static" -s -w
ldflagsversion = -X $(PACKAGE).CommitHash=$(COMMIT_HASH) -X $(PACKAGE).BuildDate=$(BUILD_DATE) -s -w

all: nbdserver tlogserver zeroctl

zeroctl: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagsversion)' -o $(OUTPUT)/$@ ./zeroctl
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags) $(ldflagsversion)' -o $(OUTPUT)/$@ ./zeroctl
endif

nbdserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagsversion)' -o $(OUTPUT)/$@ ./nbd/nbdserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags) $(ldflagsversion)' -o $(OUTPUT)/$@ ./nbd/nbdserver
endif

tlogserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagsversion)' -o $(OUTPUT)/$@ ./tlog/tlogserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflags) $(ldflagsversion)' -o $(OUTPUT)/$@ ./tlog/tlogserver
endif

test: testgo testrace testcgo testcodegen benchmarkgo

testgo:
	go test -timeout $(TIMEOUT) $(PACKAGES)

benchmarkgo:
	go test -bench=. -run=^$$ -timeout $(TIMEOUT) $(PACKAGES)

testrace: testrace_core testrace_gonbdserver testrace_tlog

testrace_core:
	go test -short -race -timeout $(TIMEOUT) $(RACE_PACKAGES)

testrace_tlog:
	go test -race -timeout $(TIMEOUT) github.com/zero-os/0-Disk/tlog/...

testrace_gonbdserver:
	go test -short -race -timeout $(TIMEOUT) github.com/zero-os/0-Disk/nbd/gonbdserver/nbd

testcodegen:
	./scripts/codegeneration.sh

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) nbdserver tlogserver zeroctl test testgo testrace testrace_core testrace_gonbdserver testrace_tlog testcgo testcodegen
