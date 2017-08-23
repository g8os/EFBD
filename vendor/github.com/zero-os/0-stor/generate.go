//go:generate protoc -I specs/protobuf  specs/protobuf/store.proto --go_out=plugins=grpc:grpc_store

package main
