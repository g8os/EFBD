using Go = import "/go.capnp";
@0xf4533cbae6e08506;
$Go.package("schema");
$Go.import("github.com/g8os/blockstor/tlog/tlogclient");

struct TlogResponse {
	status @0 :Int8;
	sequences @1 :List(UInt64);
	# only exist in flush response
}
struct TlogBlock {
	sequence @0 :UInt64;
	lba @1 :UInt64;
	size @2 :UInt64;
	hash  @3 :Data;
	data @4 :Data;
	timestamp @5 :UInt64;
	operation @6 :UInt8; # disk operation  1=WriteAt,2=WriteZeroesAt
}
struct TlogClientPackage {
	vdiskID @0 :Text;
	block @1 :TlogBlock;
}

struct TlogAggregation {
	name @0 :Text; # unused now
	size @1 :UInt64; # number of blocks in this aggregation
	timestamp @2 :UInt64;
	vdiskID @3 :Text;
	blocks @4 :List(TlogBlock);
	prev @5 :Data; # hash of the previous aggregation
}
