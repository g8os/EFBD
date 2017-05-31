using Go = import "/go.capnp";
@0xf4533cbae6e08506;
$Go.package("schema");
$Go.import("github.com/g8os/blockstor/tlog/schema");

# Handshake message sent from client to server.
struct HandshakeRequest {
	version @0 :UInt32;
	vdiskID @1 :Text;
	firstSequence @2: UInt64;
	resetFirstSequence @3: Bool;
}

# Response handshake message sent from server to client,
# after receiving a HandshakeRequest message from the client.
struct HandshakeResponse {
	version @0 :UInt32;
	status @1 :Int8;
}

struct TlogResponse {
	status @0 :Int8;
	sequences @1 :List(UInt64);
	# only exist in flush response
}
struct TlogBlock {
	sequence @0 :UInt64;
	offset @1 :UInt64;
	size @2 :UInt64;
	hash  @3 :Data;
	data @4 :Data;
	timestamp @5 :UInt64;
	operation @6 :UInt8; # disk operation  1=WriteAt,2=WriteZeroesAt
}

struct TlogAggregation {
	name @0 :Text; # unused now
	size @1 :UInt64; # number of blocks in this aggregation
	timestamp @2 :UInt64;
	vdiskID @3 :Text;
	blocks @4 :List(TlogBlock);
	prev @5 :Data; # hash of the previous aggregation
}
