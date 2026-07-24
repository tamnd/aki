package dispatch

// The write classification behind the flushlag gate (doc 04 section 6): a
// verb is a write when its handler mutates the store and emits WAL frames,
// so running it while the WAL buffer sits over cap would grow the buffer
// past the bound. The shard gate parks these before their handler runs and
// lets everything else flow, which is what keeps reads served from RAM
// through a park storm.
//
// Some rows worth their line:
//   - The blocking list verbs are writes: their happy path pops. A gated
//     BLPOP parks on flushlag first and only reaches its blocking machinery
//     once the lag clears.
//   - PFCOUNT and PFDEBUG are writes: PFCOUNT persists its cardinality
//     cache and PFDEBUG's dense conversion rewrites the sketch, and both
//     emit read-back frames (#1023).
//   - GEORADIUS and GEORADIUSBYMEMBER are writes for their STORE clauses,
//     the same classification that gives them _RO variants.
//   - XREADGROUP and the PEL verbs are writes: group state mutates and
//     frames through the groupdelta vocabulary.
//   - FLUSHALL and FLUSHDB are writes; their fan sub-command inherits the
//     bit, as do the MSET and DEL sub-commands.
var writeVerbs = []string{
	"SET", "DEL", "UNLINK", "MSET", "FLUSHALL", "FLUSHDB",
	"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
	"GETEX", "SETEX", "PSETEX",
	"INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
	"APPEND", "SETRANGE", "SETBIT", "BITFIELD", "BITOP",
	"PFADD", "PFCOUNT", "PFMERGE", "PFDEBUG",
	"SADD", "SREM", "SPOP", "SMOVE",
	"SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
	"ZADD", "ZINCRBY", "ZREM",
	"ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX",
	"ZPOPMIN", "ZPOPMAX", "ZMPOP",
	"ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
	"GEOADD", "GEOSEARCHSTORE", "GEORADIUS", "GEORADIUSBYMEMBER",
	"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP",
	"LSET", "LREM", "LTRIM", "LINSERT", "LMOVE", "RPOPLPUSH", "LMPOP",
	"BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BLMPOP",
	"HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
	"HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST",
	"XADD", "XDEL", "XSETID", "XTRIM",
	"XGROUP", "XREADGROUP", "XACK", "XCLAIM", "XAUTOCLAIM", "XNACK",
}

// WriteOps builds the op-indexed write bits Runtime.UseWriteOps installs,
// from the immutable registration table: the verb's point op and, when it
// fans, its per-shard sub-command op. Called once at server start, after
// every init has registered.
func WriteOps() []bool {
	writes := make([]bool, len(handlers))
	for _, name := range writeVerbs {
		e := table[name]
		if e == nil {
			panic("dispatch: write verb not registered: " + name)
		}
		writes[e.op] = true
		if e.fanOp != 0 {
			writes[e.fanOp] = true
		}
	}
	return writes
}
