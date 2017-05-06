OUTPUT ?= bin
GOOS ?= linux
GOARCH ?= amd64

ldflags = '-extldflags "-static"'

all: copyvdisk nbdserver

copyvdisk: $(OUTPUT)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags $(ldflags) -o $(OUTPUT)/$@ ./cmd/copyvdisk

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

.PHONY: $(OUTPUT) copyvdisk nbdserver
