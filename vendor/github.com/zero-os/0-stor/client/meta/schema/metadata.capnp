using Go = import "/go.capnp";
@0xf4533cbae6e08506;
$Go.package("schema");
$Go.import("github.com/zero-os/0-stor/client/meta/schema");

struct Metadata {
	size @0 :UInt64;
	# Size of the data in bytes

	epoch @1 :Int64;
	# creation epoch

	key @2 :Data;
	# Key used in 0-stor

	encrKey @3 :Data;
	# Encryption key used to encrypt this file

	struct Chunk {
		key @0 :Data;
		# Key used in 0-stor

		size @1 :UInt64;
		# Size of the chunk in bytes

		shards @2 :List(Text);
		# List of shards of the chunk. It's a url the 0-stor
	}
	chunks @4 :List(Chunk);
	# List of chunks of the file.
	# Can be only 1 if we don't split the file with chunker pipe

	previous @5 :Data;
	# Key to the previous metadata entry

	next @6 :Data;
	# Key to the next metadata entry

	configPtr @7 :Data;
	# Key to the configuration used by the lib to set the data.
}
