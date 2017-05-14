OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

PACKAGE = github.com/g8os/blockstor
COMMIT_HASH = $(shell git rev-parse --short HEAD 2>/dev/null)
BUILD_DATE = $(shell date +%FT%T%z)

ldflags = -extldflags "-static"
ldflagsg8stor = $(ldflags) -X $(PACKAGE)/g8stor/cmd.CommitHash=$(COMMIT_HASH) -X $(PACKAGE)/g8stor/cmd.BuildDate=$(BUILD_DATE)

all: nbdserver g8stor

g8stor: $(OUTPUT)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags '$(ldflagsg8stor)' -o $(OUTPUT)/$@ ./g8stor

nbdserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(OUTPUT)/$@ ./nbdserver
else
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags $(ldflags) -o $(OUTPUT)/$@ ./nbdserver
endif

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) nbdserver g8stor
