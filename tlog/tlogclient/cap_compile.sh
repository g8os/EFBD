#capnp compile -oc++ tlog_schema.capnp
capnp compile -I$GOPATH/src/zombiezen.com/go/capnproto2/std   -ogo tlog_schema.capnp
