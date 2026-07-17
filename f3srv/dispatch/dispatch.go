// Package dispatch is the command table: verb lookup, arity check, and the
// route into the shard runtime, all on the connection's reader goroutine.
// Errors discovered here (unknown verb, wrong arity) still travel through the
// hop as OpError so their replies keep pipeline order.
package dispatch

import (
	"github.com/tamnd/aki/engine/f3/derived"
	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/str"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
)

// entry is one command's table row. op doubles as the handler's index in the
// vector Handlers returns; name is the lowercase spelling the arity error
// quotes, so an alias like SUBSTR reports its own name.
type entry struct {
	op      byte
	name    string
	minArgs int // arguments after the verb
	maxArgs int // -1: unbounded, the handler validates the tail
	keyed   bool
	keyAt   int // index into args[1:] of the routing key; 0 for every verb
	// but OBJECT, whose key follows its subcommand token

	// The fan-out route: a non-zero fan kind scatters the command through
	// DoFan with fanOp as the per-shard sub-command op. A verb with both a
	// point op and a fan route (DEL, UNLINK, EXISTS) takes the point path for
	// one key and fans for more; MGET and MSET always fan.
	fan     shard.FanKind
	fanOp   byte
	paired  bool // MSET-shaped tail: alternating key value
	fanOnly bool // no point op; a single key still fans

	// flushOpt marks the FLUSHALL/FLUSHDB tail: the one optional argument
	// must be the ASYNC or SYNC token, anything else is a syntax error.
	flushOpt bool

	// blocks is set on a blocking verb (BLPOP and kin) so the reader arms the
	// connection barrier after enqueuing it; wired in the slice-8 blocking PR.
	blocks bool

	// cross is the tier-two cross-shard route (spec 2064/f3/03 section 6.7):
	// a command whose keys land on different shards leaves the point path and
	// runs cross under an intent transaction holding every key. Co-located
	// keys keep the point path, the free single-shard case. crossKeys extracts
	// the command's keys from its argument tail for the co-location check and
	// the transaction's intent list; nil from it (a malformed tail) keeps the
	// point path, which answers the parse error in place. SMOVE and the set
	// algebra reads ride this; RENAME, COPY, and LMOVE join with their slices.
	cross     func(t *shard.Txn, args [][]byte) []byte
	crossKeys func(args [][]byte) [][]byte

	// blockCross is the tier-two route for a blocking verb whose keys span
	// shards (spec 2064/f3/13 M3 slice 8, PR 6): DoBlockCross holds an intent on
	// every key across the serve-or-park decision, and the body serves the first
	// non-empty key under the barrier or parks a waiter on each owner and leaves
	// the reply open. It shares crossKeys with cross for the co-location check;
	// a blocking verb sets blockCross where a non-blocking tier-two verb sets
	// cross, never both. Co-located keys keep the point path, the free single-
	// shard case, which is why a colocated key set never reaches here.
	blockCross func(t *shard.Txn, conn *shard.Conn, seq uint32, args [][]byte) []byte

	// streamKeyAt routes XREAD and its kin, whose key sits after the STREAMS
	// token rather than at a fixed index: it returns the tail index of the first
	// stream key (or -1 on a malformed tail), and the single-shard read routes
	// there through DoAt. crossKeys supplies the full key set for the co-location
	// guard; the multi-shard read snapshot (the F17 hop plan) is a later slice, so
	// a key set spanning shards is refused for now rather than silently read from
	// one owner.
	streamKeyAt func(args [][]byte) int
}

// maxVerb bounds the uppercase scratch for verb lookup; no Redis verb comes
// close.
const maxVerb = 32

var (
	table    = make(map[string]*entry)
	handlers = []shard.Handler{nil} // index 0 reserved, op = position
)

// register wires one verb. Called from init only; the table is immutable
// afterwards, which is what lets Dispatch read it without a lock.
func register(name string, h shard.Handler, minArgs, maxArgs int, keyed bool) {
	lower := make([]byte, len(name))
	for i := 0; i < len(name); i++ {
		lower[i] = name[i] | 0x20
	}
	e := &entry{
		op:      byte(len(handlers)),
		name:    string(lower),
		minArgs: minArgs,
		maxArgs: maxArgs,
		keyed:   keyed,
	}
	table[name] = e
	handlers = append(handlers, h)
}

// registerShard wires a fan-out sub-command handler: it gets an op and a slot
// in the vector but no verb, so a client can never call it directly.
func registerShard(h shard.Handler) byte {
	op := byte(len(handlers))
	handlers = append(handlers, h)
	return op
}

// registerFan attaches a fan route to an already registered verb.
func registerFan(name string, kind shard.FanKind, fanOp byte, paired, fanOnly bool) {
	e := table[name]
	e.fan = kind
	e.fanOp = fanOp
	e.paired = paired
	e.fanOnly = fanOnly
}

func init() {
	register("PING", ping, 0, 1, false)
	register("ECHO", echo, 1, 1, false)

	// The string point surface. SET's tail is option soup, so the handler
	// validates it.
	register("SET", str.Set, 2, -1, true)
	register("GET", str.Get, 1, 1, true)
	register("GETDEL", str.Getdel, 1, 1, true)
	register("GETSET", str.Getset, 2, 2, true)
	register("STRLEN", str.Strlen, 1, 1, true)
	// TYPE spans the string store and the set registry, so the set package
	// owns its point handler; the same holds for the single-key EXISTS and DEL
	// paths registered below.
	register("TYPE", set.Type, 1, 1, true)

	// The tier-one multi-key commands: a single key keeps the point path,
	// more keys scatter through the fan-out; MGET and MSET always fan. The
	// sub-command handlers are shard-only ops with no verb.
	mget := registerShard(str.MGetShard)
	mset := registerShard(str.MSetShard)
	del := registerShard(str.DelShard)
	exists := registerShard(str.ExistsShard)
	register("EXISTS", set.Exists, 1, -1, true)
	register("DEL", set.Del, 1, -1, true)
	register("UNLINK", set.Del, 1, -1, true)
	register("MGET", nil, 1, -1, true)
	register("MSET", nil, 2, -1, true)
	registerFan("EXISTS", shard.FanCount, exists, false, false)
	registerFan("DEL", shard.FanCount, del, false, false)
	registerFan("UNLINK", shard.FanCount, del, false, false)
	registerFan("MGET", shard.FanMGet, mget, false, true)
	registerFan("MSET", shard.FanOK, mset, true, true)

	// INFO scatters keyless to every shard: each answers the fixed-width
	// counter blob and the gather sums the fields and renders the text. The
	// optional section argument is accepted and ignored; there is one section.
	info := registerShard(str.InfoShard)
	register("INFO", nil, 0, 1, false)
	registerFan("INFO", shard.FanStats, info, false, true)

	// DBSIZE is the same keyless scatter with the count gather: every shard
	// answers its key count and the sum is the reply.
	dbsize := registerShard(str.DBSizeShard)
	register("DBSIZE", nil, 0, 0, false)
	registerFan("DBSIZE", shard.FanCount, dbsize, false, true)

	// FLUSHALL scatters a reset intent to every shard; each owner rebuilds
	// its store empty and the gather answers +OK only after every shard has
	// confirmed, so the flush is a barrier against later commands from the
	// same connection. FLUSHDB is an alias: f3 has a single keyspace (no
	// SELECT), so flushing the db is flushing everything. The optional ASYNC
	// and SYNC tokens are both accepted and both run synchronously for now:
	// the reset is a segment rewind plus a truncate, quick enough that a
	// background reclaim buys nothing yet.
	flush := registerShard(str.FlushShard)
	register("FLUSHALL", nil, 0, 1, false)
	register("FLUSHDB", nil, 0, 1, false)
	registerFan("FLUSHALL", shard.FanOK, flush, false, true)
	registerFan("FLUSHDB", shard.FanOK, flush, false, true)
	table["FLUSHALL"].flushOpt = true
	table["FLUSHDB"].flushOpt = true

	// The INCR family, APPEND, and the range pair. SUBSTR is GETRANGE under
	// its old name; a distinct row so arity errors quote 'substr'.
	register("INCR", str.Incr, 1, 1, true)
	register("DECR", str.Decr, 1, 1, true)
	register("INCRBY", str.IncrByCmd, 2, 2, true)
	register("DECRBY", str.DecrByCmd, 2, 2, true)
	register("INCRBYFLOAT", str.IncrByFloat, 2, 2, true)
	register("APPEND", str.Append, 2, 2, true)
	register("SETRANGE", str.SetRange, 3, 3, true)
	register("GETRANGE", str.GetRange, 3, 3, true)
	register("SUBSTR", str.GetRange, 3, 3, true)

	// The bit surface (spec 2064/f3/15 M6): a bitmap is a bit-level view over
	// the string store, so the point pair rides the same keyspace as SET.
	register("SETBIT", derived.SetBit, 3, 3, true)
	register("GETBIT", derived.GetBit, 2, 2, true)
	register("BITCOUNT", derived.BitCount, 1, 4, true)
	register("BITPOS", derived.BitPos, 2, 5, true)
	register("BITFIELD", derived.BitField, 1, -1, true)
	register("BITFIELD_RO", derived.BitFieldRO, 1, -1, true)
	// BITOP <AND|OR|XOR|NOT> destkey srckey [srckey ...] (spec 2064/f3/15
	// section 5) is the one bitmap command touching more than one key. The tail
	// is the operation token, the destination, then the sources, so the routing
	// key is the destination at tail index 1 (keyAt) and the co-location check
	// spans destination plus sources (crossKeys drops the operation token). The
	// co-located case, the {tag}-hashed and single-shard norm, runs the whole
	// streaming algebra on the destination's owner through the store; keys that
	// span shards route to the F17 hop coordinator, which lands next slice and
	// until then refuses cleanly.
	register("BITOP", derived.BitOp, 3, -1, true)
	table["BITOP"].keyAt = 1
	table["BITOP"].crossKeys = func(a [][]byte) [][]byte { return a[1:] }
	table["BITOP"].cross = derived.BitOpCross

	// The HLL surface (spec 2064/f3/15 sections 7 to 9): a HyperLogLog is a
	// HYLL-format string sketch, so PFADD rides the same keyspace as SET. PFCOUNT
	// counts one key or the union of several, and PFMERGE folds sources into a
	// destination; both fold with the register-merge kernel of section 8. Their
	// co-located key sets keep the point path (PfCount/PfMerge on the one owner);
	// a key set spanning shards takes the F17 intent route of section 9, the same
	// co-located-first split BITOP took. crossKeys is the full key set, so single
	// keys never reach the cross path.
	allHLLKeys := func(a [][]byte) [][]byte { return a }
	register("PFADD", derived.PfAdd, 1, -1, true)
	register("PFCOUNT", derived.PfCount, 1, -1, true)
	table["PFCOUNT"].cross = derived.PfCountCross
	table["PFCOUNT"].crossKeys = allHLLKeys
	register("PFMERGE", derived.PfMerge, 1, -1, true)
	table["PFMERGE"].cross = derived.PfMergeCross
	table["PFMERGE"].crossKeys = allHLLKeys
	// PFDEBUG <sub> <key> introspects one sketch, so it routes on the key at tail
	// index 1 (keyAt=1), past the subcommand; PFSELFTEST touches no key and runs
	// its estimator check on any owner, so it is keyless like PING.
	register("PFDEBUG", derived.PfDebug, 2, 2, true)
	table["PFDEBUG"].keyAt = 1
	register("PFSELFTEST", derived.PfSelfTest, 0, 0, false)

	// The set surface (spec 2064/f3/11 M1). Point ops, draws, streamed
	// SMEMBERS, the downward-cursor SSCAN over all three bands, plus OBJECT
	// ENCODING for the differential encoding check. Handlers validate their
	// own tails.
	register("SADD", set.Sadd, 2, -1, true)
	register("SREM", set.Srem, 2, -1, true)
	register("SISMEMBER", set.Sismember, 2, 2, true)
	register("SMISMEMBER", set.Smismember, 2, -1, true)
	register("SCARD", set.Scard, 1, 1, true)
	register("SMEMBERS", set.Smembers, 1, 1, true)
	register("SPOP", set.Spop, 1, 2, true)
	register("SRANDMEMBER", set.Srandmember, 1, 2, true)
	register("SSCAN", set.Sscan, 2, -1, true)
	// The multi-key algebra surface (spec 2064/f3/11 section 6). SINTER, SUNION,
	// and SDIFF key on their first operand; co-located operands read from that
	// shard's registry on the point path, and operands spanning shards take the
	// F17 gather route under an intent transaction (set/gathercross.go).
	// SINTERCARD leads with numkeys, so its routing key is the argument after
	// it: keyAt=1 sends it to the first operand's shard, the same route OBJECT
	// uses for its post-subcommand key; its cross keys come from the same tail
	// parse the handler runs.
	allKeys := func(a [][]byte) [][]byte { return a }
	register("SINTER", set.Sinter, 1, -1, true)
	table["SINTER"].cross = set.SinterCross
	table["SINTER"].crossKeys = allKeys
	register("SUNION", set.Sunion, 1, -1, true)
	table["SUNION"].cross = set.SunionCross
	table["SUNION"].crossKeys = allKeys
	register("SDIFF", set.Sdiff, 1, -1, true)
	table["SDIFF"].cross = set.SdiffCross
	table["SDIFF"].crossKeys = allKeys
	register("SINTERCARD", set.Sintercard, 2, -1, false)
	table["SINTERCARD"].keyAt = 1
	table["SINTERCARD"].cross = set.SintercardCross
	table["SINTERCARD"].crossKeys = set.SintercardKeys
	// The STORE forms (spec 2064/f3/11 section 7) write the result to the
	// destination and read the sources, so they key on the destination (args[0])
	// for routing, the same first-argument route SADD uses; the sources are read
	// from the destination shard's registry, the co-located-operand constraint
	// the read-side algebra already documents (algebra_commands.go). Minimum two
	// arguments: a destination and at least one source key.
	register("SINTERSTORE", set.Sinterstore, 2, -1, true)
	register("SUNIONSTORE", set.Sunionstore, 2, -1, true)
	register("SDIFFSTORE", set.Sdiffstore, 2, -1, true)
	// SMOVE (spec 2064/f3/11 section 9.2) is a tier-two two-key write. When
	// source and destination are co-located it routes on the source (args[0],
	// the first-argument route SADD uses) and runs the whole move on that
	// owner, the free single-shard case of doc 03 section 6.1. Cross-shard it
	// rides the F17 intent path: DoTxn arms write intents on both keys in
	// inbound order and SmoveCross runs the doc 6.7 two-hop plan under the
	// barrier.
	register("SMOVE", set.Smove, 3, 3, true)
	table["SMOVE"].cross = func(t *shard.Txn, a [][]byte) []byte {
		return set.SmoveCross(t, a[0], a[1], a[2])
	}
	table["SMOVE"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	// The zset surface (spec 2064/f3/12 M2 slice 1). Point ops, ZINCRBY, ZREM,
	// rank, and ZRANGE by index over the inline band, all keyed on the first
	// argument the same way SADD is. Handlers validate their own tails.
	register("ZADD", zset.Zadd, 3, -1, true)
	register("ZSCORE", zset.Zscore, 2, 2, true)
	register("ZMSCORE", zset.Zmscore, 2, -1, true)
	register("ZCARD", zset.Zcard, 1, 1, true)
	register("ZINCRBY", zset.Zincrby, 3, 3, true)
	register("ZREM", zset.Zrem, 2, -1, true)
	register("ZRANK", zset.Zrank, 2, 3, true)
	register("ZREVRANK", zset.Zrevrank, 2, 3, true)
	register("ZRANGE", zset.Zrange, 3, -1, true)
	register("ZREVRANGE", zset.Zrevrange, 3, -1, true)
	// The by-bound range family (spec 2064/f3/12 sections 6.4, 6.5): score and
	// lex bands and their reverse forms, plus the two count verbs. All key on the
	// first argument and validate their own bound grammars and options.
	register("ZRANGEBYSCORE", zset.Zrangebyscore, 3, -1, true)
	register("ZREVRANGEBYSCORE", zset.Zrevrangebyscore, 3, -1, true)
	register("ZRANGEBYLEX", zset.Zrangebylex, 3, -1, true)
	register("ZREVRANGEBYLEX", zset.Zrevrangebylex, 3, -1, true)
	register("ZCOUNT", zset.Zcount, 3, 3, true)
	register("ZLEXCOUNT", zset.Zlexcount, 3, 3, true)
	// The range removals (spec 2064/f3/12 section 6.9): each resolves its bounds to
	// a rank window and deletes it inline, keyed on the first argument.
	register("ZREMRANGEBYRANK", zset.Zremrangebyrank, 3, 3, true)
	register("ZREMRANGEBYSCORE", zset.Zremrangebyscore, 3, 3, true)
	register("ZREMRANGEBYLEX", zset.Zremrangebylex, 3, 3, true)
	// ZSCAN walks the member records under an opaque downward cursor (section
	// 6.11), keyed on the first argument.
	register("ZSCAN", zset.Zscan, 2, -1, true)

	// The pops and random surface (spec 2064/f3/12 sections 6.7, 6.8): ZPOPMIN,
	// ZPOPMAX, and ZRANDMEMBER key on the first argument the same way ZADD does.
	// ZMPOP leads with numkeys, so its routing key is the argument after it
	// (keyAt=1), the same post-count route SINTERCARD uses; it reads every listed
	// key from that shard's registry, the co-located-operand convention. The
	// blocking forms BZPOPMIN/BZPOPMAX/BZMPOP are deferred to the F17 intent slice.
	register("ZPOPMIN", zset.Zpopmin, 1, 2, true)
	register("ZPOPMAX", zset.Zpopmax, 1, 2, true)
	register("ZRANDMEMBER", zset.Zrandmember, 1, 3, true)
	register("ZMPOP", zset.Zmpop, 3, -1, false)
	table["ZMPOP"].keyAt = 1

	// The multi-key algebra surface (spec 2064/f3/12 section 6.12). The read
	// forms ZUNION, ZINTER, ZDIFF, and ZINTERCARD lead with numkeys, so they route
	// on the first operand (keyAt=1), the post-count route SINTERCARD uses, and
	// read every operand from that shard's registry (the co-located-operand
	// convention). The STORE forms write the destination and so route on it
	// (args[0], the first-argument route SADD uses); their minimum is a
	// destination, a numkeys, and at least one source.
	register("ZUNION", zset.Zunion, 2, -1, false)
	table["ZUNION"].keyAt = 1
	register("ZINTER", zset.Zinter, 2, -1, false)
	table["ZINTER"].keyAt = 1
	register("ZDIFF", zset.Zdiff, 2, -1, false)
	table["ZDIFF"].keyAt = 1
	register("ZINTERCARD", zset.Zintercard, 2, -1, false)
	table["ZINTERCARD"].keyAt = 1
	register("ZUNIONSTORE", zset.Zunionstore, 3, -1, true)
	register("ZINTERSTORE", zset.Zinterstore, 3, -1, true)
	register("ZDIFFSTORE", zset.Zdiffstore, 3, -1, true)

	// The geo surface (spec 2064/f3/15 section 10): a geo set is a zset whose
	// score is a 52-bit geohash, so the point commands are all single-key and key
	// on the geo set argument the way ZADD does. GEOADD takes at least one triple
	// after its NX/XX/CH flags; GEOPOS and GEOHASH take a key and zero or more
	// members; GEODIST takes two members and an optional unit.
	register("GEOADD", zset.Geoadd, 4, -1, true)
	register("GEOPOS", zset.Geopos, 1, -1, true)
	register("GEODIST", zset.Geodist, 3, 4, true)
	register("GEOHASH", zset.Geohash, 1, -1, true)

	// GEOSEARCH reads a covering set of geohash ranges off the zset scores and
	// exact-filters, so it is single-key and needs a center source plus a shape.
	// The smallest valid call is the key, FROMMEMBER m, and BYRADIUS r unit, six
	// arguments after the command; the handler validates the rest.
	register("GEOSEARCH", zset.Geosearch, 6, -1, true)

	// GEOSEARCHSTORE writes the search result into a destination geo set, so it is
	// the one geo command touching two keys. The tail is the destination then the
	// source, so the routing key is the destination at args[0] (keyAt default) and
	// the co-location check spans both (crossKeys). Co-located keys run the whole
	// search-and-store on the destination's owner through the store; a destination
	// and source that span shards route to the F17 hop coordinator. The smallest
	// call is destination, source, FROMMEMBER m, and BYRADIUS r unit, seven
	// arguments after the command.
	register("GEOSEARCHSTORE", zset.Geosearchstore, 7, -1, true)
	table["GEOSEARCHSTORE"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	table["GEOSEARCHSTORE"].cross = zset.GeosearchstoreCross

	// GEORADIUS and GEORADIUSBYMEMBER are the deprecated wrappers over the same
	// engine, keyed on the source at args[0]. They read by default and turn into
	// a two-key write when STORE or STOREDIST names a destination, so their
	// crossKeys parses the tail for that destination and the F17 hop coordinator
	// handles the shard-spanning store. The smallest read is key, lon, lat,
	// radius, unit (five) for GEORADIUS and key, member, radius, unit (four) for
	// the BYMEMBER form. GEORADIUS_RO and GEORADIUSBYMEMBER_RO refuse STORE, so
	// they stay single-key and need no hop.
	register("GEORADIUS", zset.Georadius, 5, -1, true)
	table["GEORADIUS"].crossKeys = zset.GeoradiusKeys
	table["GEORADIUS"].cross = zset.GeoradiusCross
	register("GEORADIUS_RO", zset.GeoradiusRo, 5, -1, true)
	register("GEORADIUSBYMEMBER", zset.Georadiusbymember, 4, -1, true)
	table["GEORADIUSBYMEMBER"].crossKeys = zset.GeoradiusbymemberKeys
	table["GEORADIUSBYMEMBER"].cross = zset.GeoradiusbymemberCross
	register("GEORADIUSBYMEMBER_RO", zset.GeoradiusbymemberRo, 4, -1, true)

	// The list surface (spec 2064/f3/13 M3 slice 1): the inline listpack band and
	// its one-way promotion to the native quicklist placeholder. The pushes, pops,
	// LLEN, LINDEX, LRANGE, LSET, LREM, LTRIM, LINSERT, and LPOS all key on the
	// first argument the same way SADD does and validate their own tails.
	register("LPUSH", list.Lpush, 2, -1, true)
	register("RPUSH", list.Rpush, 2, -1, true)
	register("LPUSHX", list.Lpushx, 2, -1, true)
	register("RPUSHX", list.Rpushx, 2, -1, true)
	register("LPOP", list.Lpop, 1, 2, true)
	register("RPOP", list.Rpop, 1, 2, true)
	register("LLEN", list.Llen, 1, 1, true)
	register("LINDEX", list.Lindex, 2, 2, true)
	register("LRANGE", list.Lrange, 3, 3, true)
	register("LSET", list.Lset, 3, 3, true)
	register("LREM", list.Lrem, 3, 3, true)
	register("LTRIM", list.Ltrim, 3, 3, true)
	register("LINSERT", list.Linsert, 4, 4, true)
	register("LPOS", list.Lpos, 2, -1, true)
	// LMOVE and RPOPLPUSH (spec 2064/f3/13 M3 slices 6 and 7) are two-key moves.
	// When source and destination are co-located they route on the source (args[0],
	// the first-argument route the pushes use) and the whole move runs on that
	// owner's registry, the free single-shard case of doc 03 section 6.1.
	// Cross-shard (slice 7) they ride the F17 intent pair: DoTxn arms write intents
	// on both keys and LmoveCross runs the doc 6.7 plan under the barrier, capturing
	// the source end element across the hops (list/lmovecross.go). LMOVE parses its
	// two direction tokens in the cross closure the same way Lmove does, so an
	// invalid one is the syntax error before any key is touched; RPOPLPUSH is the
	// fixed RIGHT LEFT move. The blocking forms BLPOP/BRPOP/BLMPOP/BLMOVE and LMPOP
	// stay deferred to later M3 slices.
	register("LMOVE", list.Lmove, 4, 4, true)
	table["LMOVE"].cross = func(t *shard.Txn, a [][]byte) []byte {
		from, ok1 := list.ParseDir(a[2])
		to, ok2 := list.ParseDir(a[3])
		if !ok1 || !ok2 {
			return list.SyntaxError()
		}
		return list.LmoveCross(t, a[0], a[1], from, to)
	}
	table["LMOVE"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	register("RPOPLPUSH", list.Rpoplpush, 2, 2, true)
	table["RPOPLPUSH"].cross = func(t *shard.Txn, a [][]byte) []byte {
		return list.RpoplpushCross(t, a[0], a[1])
	}
	table["RPOPLPUSH"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }

	// LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count] (spec 2064/f3/13 M3
	// slice 8) is the non-blocking multi-key pop, the list twin of ZMPOP. It leads
	// with numkeys, so it routes on the argument after it (keyAt=1, the post-count
	// route ZMPOP and SINTERCARD use) and reads every listed key from that shard's
	// registry, the co-located-operand convention. It is the only non-blocking
	// member of the slice-8 family; the blocking forms BLPOP, BRPOP, BLMOVE, and
	// BLMPOP land in later slice-8 PRs on the deferred-reply and waiter substrate.
	register("LMPOP", list.Lmpop, 3, -1, false)
	table["LMPOP"].keyAt = 1

	// BLPOP/BRPOP key [key ...] timeout (spec 2064/f3/13 M3 slice 8) are the first
	// blocking list verbs. When the listed keys are co-located they key on the
	// first (keyAt 0) and read every key from that one shard's registry, the free
	// single-shard path LMPOP uses; when the keys span shards blockCross sends them
	// through DoBlockCross, which holds an intent on every key across the serve-or-
	// park decision and parks a shared-claim waiter on each owner (list/
	// blockcross.go). blocks arms the reader barrier after enqueue so a command
	// pipelined behind an unresolved BLPOP does not run until its reply goes out.
	// The immediate-serve path still replies in place; the barrier disarms itself
	// either way once emitted crosses it.
	register("BLPOP", list.Blpop, 2, -1, true)
	table["BLPOP"].blocks = true
	table["BLPOP"].blockCross = list.BlpopCross
	table["BLPOP"].crossKeys = list.BlpopKeys
	register("BRPOP", list.Brpop, 2, -1, true)
	table["BRPOP"].blocks = true
	table["BRPOP"].blockCross = list.BrpopCross
	table["BRPOP"].crossKeys = list.BlpopKeys

	// BLMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout and its older
	// spelling BRPOPLPUSH source destination timeout (spec 2064/f3/13 M3 slice 8)
	// are the blocking two-key move. Co-located keys keep the point path, which
	// routes on the source (keyAt 0, the first-argument route LMOVE and the pushes
	// use) and reads both keys from that owner's registry. When source and
	// destination span shards blockCross sends them through DoBlockCross so the
	// command holds an intent on both across the serve-or-park decision; a serving
	// push then spawns a coordinator for the cross destination hop (list/
	// blockmovecross.go). crossKeys is the two keys. blocks arms the reader barrier
	// after enqueue so a command pipelined behind an unresolved park does not run
	// until the reply goes out; an immediate serve still replies in place.
	register("BLMOVE", list.Blmove, 5, 5, true)
	table["BLMOVE"].blocks = true
	table["BLMOVE"].blockCross = list.BlmoveCross
	table["BLMOVE"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	register("BRPOPLPUSH", list.Brpoplpush, 3, 3, true)
	table["BRPOPLPUSH"].blocks = true
	table["BRPOPLPUSH"].blockCross = list.BrpoplpushCross
	table["BRPOPLPUSH"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }

	// BLMPOP timeout numkeys key [key ...] <LEFT|RIGHT> [COUNT count] (spec
	// 2064/f3/13 M3 slice 8) is the blocking LMPOP. It leads with a timeout and
	// then numkeys, so its first key sits one argument further than LMPOP's:
	// keyAt=2 routes a co-located key set to the first key's shard (LMPOP uses
	// keyAt=1) and reads every key from that owner's registry. A key set spanning
	// shards goes through blockCross with BlmpopKeys parsing the keys out of the
	// numkeys tail, the same DoBlockCross park as BLPOP. blocks arms the reader
	// barrier after enqueue, delivered on the DoAt path for the co-located case and
	// in dispatchBlockCross for the cross case.
	register("BLMPOP", list.Blmpop, 4, -1, false)
	table["BLMPOP"].keyAt = 2
	table["BLMPOP"].blocks = true
	table["BLMPOP"].blockCross = list.BlmpopCross
	table["BLMPOP"].crossKeys = list.BlmpopKeys

	// The hash surface (spec 2064/f3/10 M4 slice 1): the inline listpack band and
	// its one-way promotion to the native field table, with the point commands.
	// All key on the first argument the same way SADD does and validate their own
	// tails.
	register("HSET", hash.Hset, 3, -1, true)
	register("HMSET", hash.Hmset, 3, -1, true)
	register("HSETNX", hash.Hsetnx, 3, 3, true)
	register("HGET", hash.Hget, 2, 2, true)
	register("HMGET", hash.Hmget, 2, -1, true)
	register("HDEL", hash.Hdel, 2, -1, true)
	register("HEXISTS", hash.Hexists, 2, 2, true)
	register("HLEN", hash.Hlen, 1, 1, true)
	register("HSTRLEN", hash.Hstrlen, 2, 2, true)
	register("HINCRBY", hash.Hincrby, 3, 3, true)
	register("HINCRBYFLOAT", hash.Hincrbyfloat, 3, 3, true)
	register("HRANDFIELD", hash.Hrandfield, 1, 3, true)
	register("HSCAN", hash.Hscan, 2, -1, true)
	register("HGETALL", hash.Hgetall, 1, 1, true)
	register("HKEYS", hash.Hkeys, 1, 1, true)
	register("HVALS", hash.Hvals, 1, 1, true)
	register("HEXPIRE", hash.Hexpire, 5, -1, true)
	register("HPEXPIRE", hash.Hpexpire, 5, -1, true)
	register("HEXPIREAT", hash.Hexpireat, 5, -1, true)
	register("HPEXPIREAT", hash.Hpexpireat, 5, -1, true)
	register("HTTL", hash.Httl, 4, -1, true)
	register("HPTTL", hash.Hpttl, 4, -1, true)
	register("HEXPIRETIME", hash.Hexpiretime, 4, -1, true)
	register("HPEXPIRETIME", hash.Hpexpiretime, 4, -1, true)
	register("HPERSIST", hash.Hpersist, 4, -1, true)

	// Stream write path (M5 slice 2): the read path and groups follow. XADD keys
	// on args[0]; its minimum well-formed form is key id field value.
	register("XADD", stream.Xadd, 4, -1, true)
	register("XLEN", stream.Xlen, 1, 1, true)
	register("XDEL", stream.Xdel, 2, -1, true)
	register("XSETID", stream.Xsetid, 2, -1, true)
	// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT n]: at least key and a
	// two-token threshold clause, keyed on args[0].
	register("XTRIM", stream.Xtrim, 3, -1, true)

	// Stream read path (M5 slice 3): the counted directory seeks the window's
	// first block, then entries decode contiguously. XRANGE is key start end
	// [COUNT n]; XREVRANGE swaps the two bounds.
	register("XRANGE", stream.Xrange, 3, -1, true)
	register("XREVRANGE", stream.Xrevrange, 3, -1, true)

	// XREAD names its keys after the STREAMS token (past optional COUNT/BLOCK), so
	// it routes through streamKeyAt to the first key's shard; crossKeys guards the
	// co-location of a multi-key set. blocks arms the reader barrier after enqueue
	// so an XREAD BLOCK that parks holds its reply open until an XADD or the timeout
	// completes it, the same seam BLPOP uses; a non-blocking XREAD disarms the
	// barrier as soon as its immediate reply emits. The multi-shard read snapshot is
	// owed to a later slice.
	register("XREAD", stream.Xread, 3, -1, false)
	table["XREAD"].crossKeys = stream.ReadKeys
	table["XREAD"].streamKeyAt = stream.ReadKeyAt
	table["XREAD"].blocks = true

	// Consumer groups (M5 slice 5). XGROUP CREATE/SETID/DESTROY/CREATECONSUMER/
	// DELCONSUMER and the XINFO GROUPS read both name their key after the
	// subcommand token (XGROUP CREATE key ..., XINFO GROUPS key), so they route on
	// args[1] the way OBJECT routes on the key after its subcommand; the handler
	// validates each subcommand's own tail.
	register("XGROUP", stream.Xgroup, 1, -1, false)
	table["XGROUP"].keyAt = 1
	register("XINFO", stream.Xinfo, 1, -1, false)
	table["XINFO"].keyAt = 1

	// Group delivery (M5 slices 6 and 7). XREADGROUP names its keys after STREAMS
	// like XREAD (past the GROUP g c prefix and COUNT/BLOCK/NOACK), so it routes
	// through streamKeyAt to the first key's shard with crossKeys guarding
	// co-location. blocks arms the reader barrier so an XREADGROUP `>` BLOCK that
	// parks holds its reply open until an XADD delivers into its PEL or the timeout
	// fires, the same seam XREAD uses. XACK and XPENDING key on args[0] the way the
	// write path does.
	register("XREADGROUP", stream.Xreadgroup, 4, -1, false)
	table["XREADGROUP"].crossKeys = stream.GroupReadKeys
	table["XREADGROUP"].streamKeyAt = stream.GroupReadKeyAt
	table["XREADGROUP"].blocks = true
	register("XACK", stream.Xack, 3, -1, true)
	register("XPENDING", stream.Xpending, 2, -1, true)
	// XCLAIM key group consumer min-idle id [id ...] [opts], keyed on args[0]: an
	// in-place PEL rewrite that reassigns pending entries to a live consumer.
	register("XCLAIM", stream.Xclaim, 5, -1, true)
	// XAUTOCLAIM key group consumer min-idle start [COUNT n] [JUSTID], keyed on
	// args[0]: the scanning form that drains a stuck PEL in cursor-bounded slices.
	register("XAUTOCLAIM", stream.Xautoclaim, 5, -1, true)
	// XNACK key group <SILENT|FAIL|FATAL> IDS numids id [id ...] [RETRYCOUNT n]
	// [FORCE], keyed on args[0]: release pending entries back to the group as
	// unowned, immediately-claimable NACKs without acking them.
	register("XNACK", stream.Xnack, 6, -1, true)

	// OBJECT routes by the key after its subcommand token (OBJECT ENCODING
	// key), so it keys on args[1] of the argument tail, not args[0]. Marked
	// keyless here; the keyAt route in Dispatch sends it to the owning shard
	// when a key is present, and OBJECT HELP with no key round-robins. It routes
	// through the stream handler, which reports the stream encoding and then
	// delegates every non-stream key down the chain to the hash handler, then
	// list, then set, then the string store, so one OBJECT verb answers for every
	// type.
	register("OBJECT", stream.Object, 1, -1, false)
	table["OBJECT"].keyAt = 1
}

// Handlers returns the op-indexed handler vector for Runtime.Use.
func Handlers() []shard.Handler { return handlers }

// Demoter returns the collection-demotion hook for Runtime.UseDemoter, the entry
// the worker's demote loop calls under memory pressure to shed a native
// collection quantum to the cold tier (spec 2064/f3/06 section 6). The set, the
// zset, the list, the hash, and the stream each keep their own owner-local registry
// and footprint, so the hook fans to all five, and each weighs the other four heaps
// against the shared resident cap: the set's quantum runs over the arena plus every
// collection registry, then the zset's runs over the arena plus its own registry
// plus the others' now-shed totals, and so on down the fan. Reading each earlier
// type's ResidentBytes after it sheds is what lets a later type no-op once the types
// ahead of it have already brought the combined figure under the cap, so the one
// resident cap is a true RSS bound across the collection types rather than each type
// overrunning it by the size of the others' heaps. The list sheds before the hash
// because its interior-only policy makes it the safest to demote (it provably never
// touches a hot end), the hash sheds after because it keeps its field bytes resident
// and frees the least per quantum, and the stream sheds last, its whole-block demote
// spilling the coldest front of the log; the order is a lab knob, not a correctness
// constraint. As the remaining types grow their cold forms they join the same fan-in.
func Demoter() func(*shard.Ctx) int {
	return func(cx *shard.Ctx) int {
		n := set.DemoteQuantumOver(cx, zset.ResidentBytes(cx)+list.ResidentBytes(cx)+hash.ResidentBytes(cx)+stream.ResidentBytes(cx))
		n += zset.DemoteQuantumOver(cx, set.ResidentBytes(cx)+list.ResidentBytes(cx)+hash.ResidentBytes(cx)+stream.ResidentBytes(cx))
		n += list.DemoteQuantumOver(cx, set.ResidentBytes(cx)+zset.ResidentBytes(cx)+hash.ResidentBytes(cx)+stream.ResidentBytes(cx))
		n += hash.DemoteQuantumOver(cx, set.ResidentBytes(cx)+zset.ResidentBytes(cx)+list.ResidentBytes(cx)+stream.ResidentBytes(cx))
		n += stream.DemoteQuantumOver(cx, set.ResidentBytes(cx)+zset.ResidentBytes(cx)+list.ResidentBytes(cx)+hash.ResidentBytes(cx))
		return n
	}
}

// Dispatch routes one parsed command: uppercase the verb into a stack
// scratch, look it up, check arity, and enqueue on the connection. args are
// parser views; Do copies them into the hop node before returning, so the
// caller may reuse its read buffer immediately. The error return is fatal to
// the connection; command-level failures answer in-band.
func Dispatch(c *shard.Conn, args [][]byte) error {
	verb := args[0]
	var vb [maxVerb]byte
	if len(verb) > maxVerb {
		return unknown(c, verb)
	}
	for i := 0; i < len(verb); i++ {
		ch := verb[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		vb[i] = ch
	}
	e := table[string(vb[:len(verb)])]
	if e == nil {
		return unknown(c, verb)
	}
	n := len(args) - 1
	if n < e.minArgs || (e.maxArgs >= 0 && n > e.maxArgs) {
		return oops(c, "ERR wrong number of arguments for '"+e.name+"' command")
	}
	if e.fan != 0 && (e.fanOnly || n > 1) {
		return dispatchFan(c, e, args)
	}
	if e.cross != nil {
		if keys := e.crossKeys(args[1:]); len(keys) > 1 && !colocated(c, keys) {
			return dispatchCross(c, e, args)
		}
	}
	if e.blockCross != nil {
		if keys := e.crossKeys(args[1:]); len(keys) > 1 && !colocated(c, keys) {
			return dispatchBlockCross(c, e, args)
		}
	}
	if e.streamKeyAt != nil {
		return dispatchStreamRead(c, e, args)
	}
	if e.keyAt > 0 && n > e.keyAt {
		// A verb whose routing key is not its first argument (OBJECT) goes to
		// the shard owning args[keyAt]; without that key it falls through to the
		// keyless path below.
		err := c.DoAt(e.op, e.keyAt, args[1:])
		if err == shard.ErrTooBig {
			return oops(c, "ERR command too large")
		}
		// A blocking verb whose routing key is not its first argument (BLMPOP)
		// enqueues through DoAt and returns here, so its barrier must be armed on
		// this path too, mirroring the c.Do path below. Without this a BLMPOP that
		// parks would let a pipelined command behind it reply out of order.
		if err == nil && e.blocks {
			c.ArmBlock()
		}
		return err
	}
	err := c.Do(e.op, e.keyed, args[1:])
	if err == shard.ErrTooBig {
		// The command never entered a node, so the error reply can take its
		// pipeline slot and the connection lives on.
		return oops(c, "ERR command too large")
	}
	if err == nil && e.blocks {
		// A blocking verb enqueued: arm the reader-side barrier now that Do has
		// advanced the sequence. No verb sets blocks in this slice, so this never
		// fires; the wiring lands with BLPOP.
		c.ArmBlock()
	}
	return err
}

// dispatchStreamRead routes an XREAD-shaped command, whose routing key follows a
// STREAMS token. The single-key and co-located forms, the overwhelming majority,
// go to their one owner through DoAt at the computed key index. A key set that
// spans shards is refused until the F17 read-snapshot hop plan lands. A malformed
// tail routes keyless so the handler answers the exact parse error in place.
func dispatchStreamRead(c *shard.Conn, e *entry, args [][]byte) error {
	tail := args[1:]
	if keys := e.crossKeys(tail); len(keys) > 1 && !colocated(c, keys) {
		return oops(c, "ERR XREAD across shards is not supported yet")
	}
	idx := e.streamKeyAt(tail)
	if idx < 0 {
		err := c.Do(e.op, false, tail)
		if err == shard.ErrTooBig {
			return oops(c, "ERR command too large")
		}
		return err
	}
	err := c.DoAt(e.op, idx, tail)
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	if err == nil && e.blocks {
		c.ArmBlock()
	}
	return err
}

// dispatchFan scatters one multi-key command. The fan path allocates its key
// slices; it is the multi-key surface, not the point path.
func dispatchFan(c *shard.Conn, e *entry, args [][]byte) error {
	if !e.keyed {
		// A keyless fan (INFO, FLUSHALL) scatters to every shard rather than
		// routing by key.
		if e.flushOpt && len(args) == 2 && !flushToken(args[1]) {
			return oops(c, "ERR syntax error")
		}
		err := c.DoFanAll(e.fanOp, e.fan)
		if err == shard.ErrTooBig {
			return oops(c, "ERR command too large")
		}
		return err
	}
	var keys, vals [][]byte
	if e.paired {
		n := len(args) - 1
		if n%2 != 0 {
			return oops(c, "ERR wrong number of arguments for '"+e.name+"' command")
		}
		k := n / 2
		keys = make([][]byte, k)
		vals = make([][]byte, k)
		for i := 0; i < k; i++ {
			keys[i] = args[1+2*i]
			vals[i] = args[2+2*i]
		}
	} else {
		keys = args[1:]
	}
	err := c.DoFan(e.fanOp, e.fan, keys, vals)
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	return err
}

// colocated reports whether every key routes to one shard, the check that
// keeps a tier-two command on its free single-shard fast path.
func colocated(c *shard.Conn, keys [][]byte) bool {
	for _, k := range keys[1:] {
		if !c.SameShard(keys[0], k) {
			return false
		}
	}
	return true
}

// dispatchCross routes one tier-two command whose keys span shards: the
// argument tail is copied (the transaction body runs on its own goroutine
// after these parser views die) and DoTxn arms intents on every key, runs
// e.cross under the barrier, and delivers the reply at this command's
// pipeline slot. Cross-shard tier-two traffic is rare, so the copies are off
// every hot path.
func dispatchCross(c *shard.Conn, e *entry, args [][]byte) error {
	a := copyTail(args)
	err := c.DoTxn(e.crossKeys(a), func(t *shard.Txn) []byte {
		return e.cross(t, a)
	})
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	return err
}

// dispatchBlockCross routes one blocking tier-two command whose keys span
// shards: like dispatchCross it copies the argument tail (the body runs on its
// own goroutine after the parser views die) and DoBlockCross arms intents on
// every key, but the body may park instead of replying. On a clean enqueue the
// reader barrier is armed one past the command's sequence, byte for byte like the
// point blocking path, so a command pipelined behind an unresolved park does not
// reply out of order; an immediate serve inside the body still lands its reply at
// this slot and the barrier disarms itself once emitted crosses it.
func dispatchBlockCross(c *shard.Conn, e *entry, args [][]byte) error {
	a := copyTail(args)
	err := c.DoBlockCross(e.crossKeys(a), func(t *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		return e.blockCross(t, conn, seq, a)
	})
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	if err == nil {
		c.ArmBlock()
	}
	return err
}

// copyTail copies a command's argument tail (everything after the verb) into
// fresh storage, the stable copy a tier-two body reads after the connection's
// parser views are reused. Cross-shard tier-two traffic is rare, so the copy is
// off every hot path.
func copyTail(args [][]byte) [][]byte {
	a := make([][]byte, len(args)-1)
	for i := range a {
		a[i] = append([]byte(nil), args[i+1]...)
	}
	return a
}

// flushToken reports whether arg is the ASYNC or SYNC option, case folded.
// Both are accepted; dispatchFan runs either synchronously.
func flushToken(arg []byte) bool {
	return tokenIs(arg, "ASYNC") || tokenIs(arg, "SYNC")
}

// tokenIs is a case-insensitive ASCII compare against an uppercase word.
func tokenIs(arg []byte, word string) bool {
	if len(arg) != len(word) {
		return false
	}
	for i := 0; i < len(word); i++ {
		ch := arg[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		if ch != word[i] {
			return false
		}
	}
	return true
}

func unknown(c *shard.Conn, verb []byte) error {
	return oops(c, "ERR unknown command '"+string(verb)+"'")
}

// oops enqueues an in-order error reply. Error paths allocate; the hot path
// never comes here.
func oops(c *shard.Conn, msg string) error {
	return c.Do(shard.OpError, false, [][]byte{[]byte(msg)})
}

// ping answers PONG bare and echoes a payload, the Redis shape.
func ping(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args) == 0 {
		r.Status("PONG")
		return
	}
	r.Bulk(args[0])
}

func echo(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Bulk(args[0])
}
