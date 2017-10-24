package command

// Type describes a command type
type Type struct {
	Name  string
	Write bool
}

var (
	// Decrement decrements the integer value of a key by one.
	Decrement = Type{"DECR", true}

	// DecrementBy decrements the integer value of a key by the given number.
	DecrementBy = Type{"DECRBY", true}

	// Delete a key.
	Delete = Type{"DEL", true}

	// Dump returns a serialized version of the value stored at the specified key.
	Dump = Type{"DUMP", false}

	// Exists determines if a key exists.
	Exists = Type{"EXISTS", false}

	// Get the value of a key.
	Get = Type{"GET", false}

	// Scan iterates through all values in a database.
	Scan = Type{"SCAN", false}

	// HashDelete deletes one or more hash fields.
	HashDelete = Type{"HDEL", true}

	// HashExists determines if a hash field exists.
	HashExists = Type{"HEXISTS", false}

	// HashGet gets the value of a hash field.
	HashGet = Type{"HGET", false}

	// HashGetAll gets all the fields and values in a hash.
	HashGetAll = Type{"HGETALL", false}

	// HashIncrementBy increments the integer value
	// of a hash field by the given number.
	HashIncrementBy = Type{"HINCRBY", true}

	// HashKeys gets all the fields in a hash.
	HashKeys = Type{"HKEYS", false}

	// HashLength gets the number of fields in a hash.
	HashLength = Type{"HLEN", false}

	// HashSet sets the value of a hash field
	HashSet = Type{"HSET", true}

	// HashValues gets the values in a hash.
	HashValues = Type{"HVALS", false}

	// HashScan iterates through all values in a hash.
	HashScan = Type{"HSCAN", false}

	// Increment the integer value of a key by one.
	Increment = Type{"INCR", true}

	// IncrementBy increments the integer value of a key by the given amount.
	IncrementBy = Type{"INCRBY", true}

	// ListIndex gets an element from a list by its index.
	ListIndex = Type{"LINDEX", false}

	// ListInsert inserts an element before or after another element in a list.
	ListInsert = Type{"LINSERT", true}

	// ListLength gets the length of a list.
	ListLength = Type{"LLEN", false}

	// ListPop removes and gets the first element in a list.
	ListPop = Type{"LPOP", true}

	// ListPush prepends one or multiple values to a list.
	ListPush = Type{"LPUSH", true}

	// ListRemove removes elements from a list.
	ListRemove = Type{"LREM", true}

	// ListSet sets the value of an element in a list by its index.
	ListSet = Type{"LSET", true}

	// Rename a key.
	Rename = Type{"RENAME", true}

	// ReversePop removes and gets the last element in a list.
	ReversePop = Type{"RPOP", true}

	// ReversePush appends one or multiple values to a list.
	ReversePush = Type{"RPUSH", true}

	// SetAdd adds one or more members to a set.
	SetAdd = Type{"SADD", true}

	// SetCardinal gets the number of members in a set.
	SetCardinal = Type{"SCARD", false}

	// SetDifference subtracts multiple sets
	SetDifference = Type{"SDIFF", false}

	// SetDifferenceStore subtracts multiple sets
	// and stores the resulting set in a key.
	SetDifferenceStore = Type{"SDIFFSTORE", true}

	// Set the value of a key.
	Set = Type{"SET", true}

	// SetIntersect intersects multiple sets
	SetIntersect = Type{"SINTER", false}

	// SetIntersectStore intersects multiple sets
	// and stores the resulting set in a key.
	SetIntersectStore = Type{"SINTERSTORE", true}

	// SetIsMember determines if a given value is a member of a set.
	SetIsMember = Type{"SISMEMBER", false}

	// SetMembers gets all the members in a set.
	SetMembers = Type{"SMEMBERS", false}

	// SetMove moves a member from one set to another.
	SetMove = Type{"SMOVE", true}

	// Sort the elements in a list, set or sorted set.
	Sort = Type{"SORT", true}

	// SetPop removes and returns one or multiple random members from a set.
	SetPop = Type{"SPOP", true}

	// SetRandomMember gets one or multiple random members from a set.
	SetRandomMember = Type{"SRANDMEMBER", false}

	// SetRemove removes one or more members from a set.
	SetRemove = Type{"SREM", true}

	// SetUnion adds multiple sets.
	SetUnion = Type{"SUNION", false}

	// SetUnionStore adds multiple sets and stores the resulting set in a key.
	SetUnionStore = Type{"SUNIONSTORE", false}
)
