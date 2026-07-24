// Package conformance holds the per-command hot corpus shared by the
// binary-level suite in cmd/obs1srv and the fold and restart arms in
// obs1srv/drivers (spec 2064/obs1 doc 10, suite conformance). One
// connection replays the corpus in order, so later steps may lean on
// earlier state. Every registered verb appears at least once (the
// binary suite enforces it against dispatch.Commands), happy path plus
// the shared error shapes. Keys are unique per family and spread over
// slots, so the fan, point, and cross-slot routes all serve.
package conformance

// Step is one corpus entry: a command and its rendered expected reply.
// A Want beginning with "~" is a substring match, for replies that
// carry counters or version-shaped text (INFO, XINFO). DurableWant,
// when set, replaces Want on a durability-booted node: the corpus was
// written against the volatile binary, and the durability family
// answers differently when a write log is actually underneath.
type Step struct {
	Cmd         []string
	Want        string
	DurableWant string
}

// c is shorthand for a corpus step.
func c(want string, cmd ...string) Step { return Step{Cmd: cmd, Want: want} }

// d is a corpus step whose reply differs on a durable node.
func d(want, durableWant string, cmd ...string) Step {
	return Step{Cmd: cmd, Want: want, DurableWant: durableWant}
}

// Hot is the corpus. WipeTail is how many steps at the end wipe the
// keyspace and prove it empties; arms that fingerprint the final state
// replay Hot[:len(Hot)-WipeTail] instead.
const WipeTail = 3

var Hot = []Step{
	// Connection basics.
	c("PONG", "PING"),
	c("hello", "PING", "hello"),
	c("echoed", "ECHO", "echoed"),

	// Strings: the point surface. DBSIZE runs while exactly one key
	// exists, so the expectation holds regardless of the inherited f3
	// finding that typed keys are missing from the count.
	c("OK", "SET", "k", "v"),
	c("1", "DBSIZE"),
	c("v", "GET", "k"),
	c("1", "EXISTS", "k"),
	c("string", "TYPE", "k"),
	c("6", "APPEND", "k", "-tail"),
	c("v-tail", "GET", "k"),
	c("6", "STRLEN", "k"),
	c("6", "SETRANGE", "k", "2", "X"),
	c("v-Xail", "GETRANGE", "k", "0", "-1"),
	c("Xail", "SUBSTR", "k", "2", "5"),
	c("1", "INCR", "n"),
	c("5", "INCRBY", "n", "4"),
	c("4", "DECR", "n"),
	c("2", "DECRBY", "n", "2"),
	c("2.5", "INCRBYFLOAT", "n", "0.5"),
	c("OK", "MSET", "ma", "1", "mb", "2"),
	c("[1 2 (nil)]", "MGET", "ma", "mb", "missing"),
	c("1", "DEL", "ma"),
	c("1", "UNLINK", "mb"),

	// Key-level TTL family. Deadlines are absolute far-future stamps so
	// every read is exact; the relative forms only answer their set
	// verdicts or the -1/-2 sentinels, never a counting-down remainder.
	c("0", "EXPIRE", "noexp", "100"),
	c("0", "PEXPIRE", "noexp", "100000"),
	c("-2", "TTL", "noexp"),
	c("-2", "PTTL", "noexp"),
	c("-2", "EXPIRETIME", "noexp"),
	c("-2", "PEXPIRETIME", "noexp"),
	c("0", "PERSIST", "noexp"),
	c("OK", "SET", "exp", "v"),
	c("-1", "TTL", "exp"),
	c("-1", "PTTL", "exp"),
	c("1", "EXPIRE", "exp", "100"),
	c("1", "PEXPIRE", "exp", "100000"),
	c("1", "EXPIREAT", "exp", "33177600000"),
	c("33177600000", "EXPIRETIME", "exp"),
	c("33177600000000", "PEXPIRETIME", "exp"),
	c("0", "EXPIREAT", "exp", "33177600001", "NX"),
	c("0", "EXPIREAT", "exp", "33177600000", "GT"),
	c("1", "EXPIREAT", "exp", "33177600001", "GT"),
	c("1", "PEXPIREAT", "exp", "33177600000000", "LT"),
	c("1", "PERSIST", "exp"),
	c("0", "PERSIST", "exp"),
	c("-1", "TTL", "exp"),
	c("1", "PEXPIREAT", "exp", "1"),
	c("0", "EXISTS", "exp"),
	c("-2", "TTL", "exp"),
	c("OK", "SETEX", "sx", "100", "v1"),
	c("OK", "PSETEX", "sx", "100000", "v2"),
	c("v2", "GETEX", "sx", "PERSIST"),
	c("-1", "TTL", "sx"),
	c("v2", "GETEX", "sx", "EXAT", "33177600000"),
	c("33177600000", "EXPIRETIME", "sx"),
	c("v2", "GETEX", "sx"),
	c("(nil)", "GETEX", "nosx"),
	c("v2", "GETEX", "sx", "PXAT", "1"),
	c("0", "EXISTS", "sx"),
	// A far-future deadline that stays put, so the fold and restart arms
	// carry a TTL-bearing string through their fingerprints.
	c("OK", "SET", "expkeep", "v", "PXAT", "33177600000000"),
	c("33177600000000", "PEXPIRETIME", "expkeep"),

	// Bits.
	c("0", "SETBIT", "bits", "7", "1"),
	c("1", "GETBIT", "bits", "7"),
	c("1", "BITCOUNT", "bits"),
	c("7", "BITPOS", "bits", "1"),
	c("1", "BITOP", "AND", "bdest", "bits", "bits"),
	c("[0]", "BITFIELD", "bf", "SET", "u8", "0", "255"),
	c("[255]", "BITFIELD", "bf", "GET", "u8", "0"),
	c("[255]", "BITFIELD_RO", "bf", "GET", "u8", "0"),

	// Hash, including the field-TTL family.
	c("2", "HSET", "h", "f1", "v1", "f2", "v2"),
	c("v1", "HGET", "h", "f1"),
	c("[v1 v2]", "HMGET", "h", "f1", "f2"),
	c("1", "HEXISTS", "h", "f1"),
	c("2", "HLEN", "h"),
	c("2", "HSTRLEN", "h", "f1"),
	c("1", "HDEL", "h", "f2"),
	c("[f1]", "HKEYS", "h"),
	c("[v1]", "HVALS", "h"),
	c("[f1 v1]", "HGETALL", "h"),
	c("f1", "HRANDFIELD", "h"),
	c("[0 [f1 v1]]", "HSCAN", "h", "0"),
	c("OK", "HMSET", "h2", "g", "10"),
	c("1", "HSETNX", "h2", "g2", "x"),
	c("0", "HSETNX", "h2", "g2", "y"),
	c("15", "HINCRBY", "h2", "g", "5"),
	c("15.5", "HINCRBYFLOAT", "h2", "g", "0.5"),
	c("[1]", "HEXPIRE", "h2", "100", "FIELDS", "1", "g"),
	c("[100]", "HTTL", "h2", "FIELDS", "1", "g"),
	c("[1]", "HPERSIST", "h2", "FIELDS", "1", "g"),
	c("[-1]", "HPTTL", "h2", "FIELDS", "1", "g"),
	c("[1]", "HEXPIREAT", "h2", "33177600000", "FIELDS", "1", "g"),
	c("[33177600000]", "HEXPIRETIME", "h2", "FIELDS", "1", "g"),
	c("[1]", "HPEXPIREAT", "h2", "33177600000000", "FIELDS", "1", "g"),
	c("[33177600000000]", "HPEXPIRETIME", "h2", "FIELDS", "1", "g"),
	c("[1]", "HPEXPIRE", "h2", "100000", "FIELDS", "1", "g"),

	// List, then the blocking forms in their immediate-serve shape.
	c("3", "RPUSH", "l", "a", "b", "c"),
	c("4", "LPUSH", "l", "z"),
	c("5", "RPUSHX", "l", "d"),
	c("6", "LPUSHX", "l", "y"),
	c("0", "LPUSHX", "nolist", "y"),
	c("6", "LLEN", "l"),
	c("[y z a b c d]", "LRANGE", "l", "0", "-1"),
	c("a", "LINDEX", "l", "2"),
	c("2", "LPOS", "l", "a"),
	c("OK", "LSET", "l", "0", "Y"),
	c("7", "LINSERT", "l", "BEFORE", "z", "w"),
	c("Y", "LPOP", "l"),
	c("d", "RPOP", "l"),
	c("[w z]", "LPOP", "l", "2"),
	c("1", "LREM", "l", "0", "a"),
	c("OK", "LTRIM", "l", "0", "0"),
	c("[b]", "LRANGE", "l", "0", "-1"),
	c("b", "LMOVE", "l", "l2", "LEFT", "RIGHT"),
	c("b", "RPOPLPUSH", "l2", "l3"),
	// Inherited f3 shape: the non-blocking LMPOP and ZMPOP immediate
	// paths only see keys co-located with the routed shard, so an empty
	// first key on another shard hides a populated second one (f3srv
	// answers the same; its wyhash just co-locates different pairs). The
	// populated key rides first here. The blocking forms arm watchers on
	// every shard and serve cross-shard fine, so BLMPOP below keeps the
	// empty key first on purpose.
	c("[l3 [b]]", "LMPOP", "2", "l3", "nolist", "LEFT"),
	c("1", "RPUSH", "bl", "one"),
	c("[bl one]", "BLPOP", "bl", "0"),
	c("1", "RPUSH", "bl", "two"),
	c("[bl two]", "BRPOP", "bl", "0"),
	c("(nil)", "BLPOP", "bl", "0.05"),
	c("1", "RPUSH", "bl", "three"),
	c("three", "BLMOVE", "bl", "bl2", "LEFT", "RIGHT", "0"),
	c("three", "BRPOPLPUSH", "bl2", "bl3", "0"),
	c("[bl3 [three]]", "BLMPOP", "0", "2", "nolist", "bl3", "LEFT"),
	// The same empty-key-first pair through BLPOP's cross-shard
	// immediate serve, so its pop framing is under the restart arm too.
	c("1", "RPUSH", "bl3", "four"),
	c("[bl3 four]", "BLPOP", "nolist", "bl3", "0"),

	// Set, with single-member sets where the reply order would
	// otherwise be theirs to choose. sttl keeps a far-future root
	// deadline to its last breath, so the fold and restart arms carry a
	// TTL-bearing collection through their fingerprints.
	c("1", "SADD", "sttl", "m"),
	c("1", "EXPIREAT", "sttl", "33177600000"),
	c("33177600000", "EXPIRETIME", "sttl"),
	c("2", "SADD", "s", "m1", "m2"),
	c("2", "SCARD", "s"),
	c("1", "SISMEMBER", "s", "m1"),
	c("[1 0]", "SMISMEMBER", "s", "m1", "nope"),
	c("1", "SREM", "s", "m2"),
	c("[m1]", "SMEMBERS", "s"),
	c("m1", "SRANDMEMBER", "s"),
	c("[0 [m1]]", "SSCAN", "s", "0"),
	// The algebra reads fan across shards; the store variants share the
	// pops' inherited shard scope, so their keys wear one hash tag and
	// co-locate (obs1 slot routing honors tags; two-key moves like SMOVE
	// and LMOVE coordinate cross-shard and stay untagged).
	c("1", "SADD", "sa", "x"),
	c("1", "SADD", "sb", "y"),
	c("[x]", "SDIFF", "sa", "sb"),
	c("[]", "SINTER", "sa", "sb"),
	c("0", "SINTERCARD", "2", "sa", "sb"),
	c("[x y]", "SUNION", "sa", "sb"),
	c("1", "SADD", "{sg}a", "x"),
	c("1", "SADD", "{sg}b", "y"),
	c("1", "SDIFFSTORE", "{sg}d", "{sg}a", "{sg}b"),
	c("0", "SINTERSTORE", "{sg}i", "{sg}a", "{sg}b"),
	c("2", "SUNIONSTORE", "{sg}u", "{sg}a", "{sg}b"),
	c("1", "SMOVE", "sa", "sb", "x"),
	c("1", "SISMEMBER", "sb", "x"),
	c("x", "SPOP", "{sg}d"),

	// Sorted set.
	c("2", "ZADD", "z", "1", "a", "2", "b"),
	c("2", "ZCARD", "z"),
	c("1", "ZSCORE", "z", "a"),
	c("[1 2 (nil)]", "ZMSCORE", "z", "a", "b", "nope"),
	c("2", "ZCOUNT", "z", "-inf", "+inf"),
	c("3", "ZINCRBY", "z", "2", "a"),
	c("0", "ZRANK", "z", "b"),
	c("0", "ZREVRANK", "z", "a"),
	c("[b a]", "ZRANGE", "z", "0", "-1"),
	c("[b a]", "ZRANGEBYSCORE", "z", "-inf", "+inf"),
	c("[a b]", "ZREVRANGE", "z", "0", "-1"),
	c("[a b]", "ZREVRANGEBYSCORE", "z", "+inf", "-inf"),
	// z's scores differ (b=2, a=3), so the lex walks come back in score
	// order; real Redis calls the unequal-score BYLEX case unspecified.
	c("[b a]", "ZRANGEBYLEX", "z", "-", "+"),
	c("[a b]", "ZREVRANGEBYLEX", "z", "+", "-"),
	c("2", "ZLEXCOUNT", "z", "-", "+"),
	c("[0 [b 2 a 3]]", "ZSCAN", "z", "0"),
	c("1", "ZADD", "z1", "5", "solo"),
	c("solo", "ZRANDMEMBER", "z1"),
	c("[solo 5]", "ZPOPMIN", "z1"),
	c("1", "ZADD", "z1", "5", "solo"),
	c("[solo 5]", "ZPOPMAX", "z1"),
	c("1", "ZADD", "z1", "5", "solo"),
	c("[z1 [[solo 5]]]", "ZMPOP", "2", "z1", "nokey", "MIN"),
	c("1", "ZADD", "za", "1", "p"),
	c("1", "ZADD", "zb", "1", "q"),
	c("[p]", "ZDIFF", "2", "za", "zb"),
	c("[]", "ZINTER", "2", "za", "zb"),
	c("0", "ZINTERCARD", "2", "za", "zb"),
	c("[p q]", "ZUNION", "2", "za", "zb"),
	c("1", "ZADD", "{zg}a", "1", "p"),
	c("1", "ZADD", "{zg}b", "1", "q"),
	c("1", "ZDIFFSTORE", "{zg}d", "2", "{zg}a", "{zg}b"),
	c("0", "ZINTERSTORE", "{zg}i", "2", "{zg}a", "{zg}b"),
	c("2", "ZUNIONSTORE", "{zg}u", "2", "{zg}a", "{zg}b"),
	c("1", "ZREM", "z", "b"),
	c("0", "ZREMRANGEBYRANK", "{zg}d", "5", "6"),
	c("1", "ZREMRANGEBYSCORE", "{zg}d", "-inf", "+inf"),
	c("0", "ZREMRANGEBYLEX", "{zg}d", "-", "+"),

	// Geo, on the Sicily pair the drivers corpus uses.
	c("2", "GEOADD", "geo", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"),
	c("166274.1516", "GEODIST", "geo", "Palermo", "Catania"),
	c("166.2742", "GEODIST", "geo", "Palermo", "Catania", "km"),
	c("[sqc8b49rny0 sqdtr74hyu0]", "GEOHASH", "geo", "Palermo", "Catania"),
	c("[[13.361389338970184 38.115556395496299]]", "GEOPOS", "geo", "Palermo"),
	c("[Catania Palermo]", "GEOSEARCH", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"),
	c("2", "GEOSEARCHSTORE", "geodst", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"),
	c("[Catania Palermo]", "GEORADIUS", "geo", "15", "37", "200", "km", "ASC"),
	c("[Catania Palermo]", "GEORADIUS_RO", "geo", "15", "37", "200", "km", "ASC"),
	c("[Palermo Catania]", "GEORADIUSBYMEMBER", "geo", "Palermo", "200", "km", "ASC"),
	c("[Palermo Catania]", "GEORADIUSBYMEMBER_RO", "geo", "Palermo", "200", "km", "ASC"),

	// HyperLogLog.
	c("1", "PFADD", "hll", "a", "b", "c"),
	c("3", "PFCOUNT", "hll"),
	c("OK", "PFMERGE", "hlldst", "hll"),
	c("3", "PFCOUNT", "hlldst"),
	c("OK", "PFSELFTEST"),
	c("OK", "PFDEBUG", "TODENSE", "hll"),

	// Stream: append, read, then the consumer-group loop.
	c("1-1", "XADD", "st", "1-1", "f", "v"),
	c("1", "XLEN", "st"),
	c("[[1-1 [f v]]]", "XRANGE", "st", "-", "+"),
	c("[[1-1 [f v]]]", "XREVRANGE", "st", "+", "-"),
	c("[[st [[1-1 [f v]]]]]", "XREAD", "COUNT", "1", "STREAMS", "st", "0"),
	c("OK", "XGROUP", "CREATE", "st", "grp", "0"),
	c("[[st [[1-1 [f v]]]]]", "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "1", "STREAMS", "st", ">"),
	c("[1 1-1 1-1 [[c1 1]]]", "XPENDING", "st", "grp"),
	c("[[1-1 [f v]]]", "XCLAIM", "st", "grp", "c2", "0", "1-1"),
	c("1", "XNACK", "st", "grp", "SILENT", "IDS", "1", "1-1"),
	c("[0-0 [[1-1 [f v]]] []]", "XAUTOCLAIM", "st", "grp", "c3", "0", "0"),
	c("1", "XACK", "st", "grp", "1-1"),
	c("~length", "XINFO", "STREAM", "st"),
	c("OK", "XSETID", "st", "2-0"),
	c("2-1", "XADD", "st", "2-1", "g", "w"),
	c("1", "XDEL", "st", "2-1"),
	c("1", "XTRIM", "st", "MAXLEN", "0"),

	// Introspection and admin.
	c("embstr", "OBJECT", "ENCODING", "k"),
	c("~used_memory", "INFO"),

	// Durability: the conformance binary runs persistence off, so the
	// node is volatile. The bare query reports relaxed, RELAXED is an
	// accepted no-op, and STRICT is refused because there is no commit
	// chain to wait on.
	c("relaxed", "AKI.DURABILITY"),
	c("OK", "AKI.DURABILITY", "RELAXED"),
	d("ERR DURABILITY STRICT is not available on a volatile node", "OK", "AKI.DURABILITY", "STRICT"),

	// The barriers on a volatile node: WAIT's ask of nothing answers the
	// achieved 0 in place, WAITAOF's ask of nothing answers the honest
	// [0, 0], and a numlocal ask is refused in Redis's words. Positive
	// asks park, so the corpus stays on the immediate shapes.
	c("0", "WAIT", "0", "0"),
	d("[0 0]", "[0 0]|[1 0]", "WAITAOF", "0", "0", "0"),
	d("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.", "[1 0]", "WAITAOF", "1", "0", "0"),

	// The shared error shapes, one of each.
	c("ERR unknown command 'NOPE'", "NOPE"),
	c("ERR wrong number of arguments for 'get' command", "GET"),
	// Typed commands check the string store, so this direction raises
	// WRONGTYPE. The reverse (GET on a hash key) answers nil because the
	// string store is blind to the typed heaps, the same inherited f3
	// shape as the DBSIZE undercount; f3srv answers both the same way.
	c("WRONGTYPE Operation against a key holding the wrong kind of value", "HGET", "k", "f"),

	// Wipe both spellings and prove the store empties.
	c("OK", "FLUSHDB"),
	c("0", "DBSIZE"),
	c("OK", "FLUSHALL"),
}
