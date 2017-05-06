OUTPUT ?= bin

GOOS ?= linux
GOARCH ?= amd64

ldflags = '-extldflags "-static"'

all: copyvdisks nbdserver

copyvdisks: $(OUTPUT)
	cd cmd/copyvdisk && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags $(ldflags) -o ../../$(OUTPUT)/$@

nbdserver: $(OUTPUT)
ifeq ($(GOOS), darwin)
	cd nbdserver && GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o ../$(OUTPUT)/$@
else
	cd nbdserver && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags $(ldflags) -o ../$(OUTPUT)/$@
endif

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) copyvdisks nbdserver
