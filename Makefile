OUTPUT = bin

ldflags = '-extldflags "-static"'

all: copyvdisks nbdserver

copyvdisks: $(OUTPUT)
	cd cmd/copyvdisk && CGO_ENABLED=0 GOOS=linux go build -ldflags $(ldflags) -o ../../$(OUTPUT)/$@

nbdserver: $(OUTPUT)
	cd nbdserver && CGO_ENABLED=0 GOOS=linux go build -ldflags $(ldflags) -o ../$(OUTPUT)/$@

$(OUTPUT):
	mkdir -p $(OUTPUT)

.PHONY: $(OUTPUT) copyvdisks nbdserver
