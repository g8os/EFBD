# TLOG Server

The server code is in the [`tlogserver`](tlogserver/) directory, see the [README](tlogserver/README.md) there for more details.

There is also a TLOG client, implemented in [`tloglient`](tlogclient) directory, see the [README](tlogclient/readme.md) there for more details.

## Code Generation

Cap'n Proto code (`schema/tlog_schema.capnp.go`) can be generated using `go generate`,
make sure to have capnp 0.6.0 installed by running `./install_capnp_0.6.0.sh`.

Always make sure to keep the generated code up to date with its relevant schema!

## More

More verbose documentation can be found in the [`/docs`](/docs) directory.
