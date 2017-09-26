using Go = import "/go.capnp";
@0xf4533cbae6e08506;
$Go.package("schema");
$Go.import("github.com/zero-os/0-Disk/tlog/schema");

# Handshake message sent from client to server.
struct HandshakeRequest {
	version @0 :UInt32;
	vdiskID @1 :Text;
}

# Response handshake message sent from server to client,
# after receiving a HandshakeRequest message from the client.
struct HandshakeResponse {
	version @0 :UInt32;
	status @1 :Int8;
	lastFlushedSequence @2: UInt64;
}

# tlog block aggregation
struct TlogAggregation {
	name @0 :Text; # unused now
	size @1 :UInt64; # number of blocks in this aggregation
	timestamp @2 :Int64;
	blocks @3 :List(TlogBlock);
	prev @4 :Data; # hash of the previous aggregation
}

# message to send from client to server
struct TlogClientMessage {
	union {
		block @0 :TlogBlock;        # block message
		forceFlushAtSeq @1 :UInt64; # force flush at seq message
		waitNBDSlaveSync @2 :Void;  # Wait NBD Slave Sync message
        disconnect @3 :Void;        # disconnect from server
	}
}

# a tlog block
struct TlogBlock {
	sequence @0 :UInt64;
	index @1 :Int64;
	hash  @2 :Data;
	data @3 :Data;
	timestamp @4 :Int64;
	operation @5 :UInt8; # disk operation  1=OpSet,2=OpDelete
}

# Response from server to client
struct TlogResponse {
	status @0 :Int8;
	sequences @1 :List(UInt64); # can be nil
}
