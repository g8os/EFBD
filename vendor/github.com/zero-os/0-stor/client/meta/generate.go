package meta

//go:generate capnp compile -I$GOPATH/src/zombiezen.com/go/capnproto2/std -ogo schema/metadata.capnp
//go:generate protoc --proto_path=schema --go_out=schema schema/metadata.proto
