// Package dispatch is the command table: verb lookup, arity check, and the
// route into the shard runtime, all on the connection's reader goroutine.
// Errors discovered here (unknown verb, wrong arity) still travel through the
// hop as OpError so their replies keep pipeline order.
package dispatch

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"sort"
	"strconv"

	"github.com/tamnd/aki/engine/f3/derived"
	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/str"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
	"github.com/tamnd/aki/f3srv/resp"
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
	fanArgs bool // keyless fan that still carries its argument tail (KEYS)

	// flushOpt marks the FLUSHALL/FLUSHDB tail: the one optional argument
	// must be the ASYNC or SYNC token, anything else is a syntax error.
	flushOpt bool

	// scanOpts marks the SCAN tail: a cursor followed by MATCH/COUNT/TYPE
	// options the fan validates once before scattering, so a bad cursor or a
	// malformed option answers a single error rather than one partial per shard.
	scanOpts bool

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

	// subFan routes a verb whose keyless subcommands aggregate across every
	// shard while its keyed subcommands stay on the point path (MEMORY: USAGE
	// keys on args[1], STATS and DOCTOR fan-all). It returns the fan kind and
	// true for a subcommand that scatters keyless, or ok=false to leave the
	// command on the point route. fanOp carries the per-shard sub-command op.
	subFan func(args [][]byte) (shard.FanKind, bool)
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
	register("TIME", timeCmd, 0, 0, false)
	register("SELECT", selectDB, 1, 1, false)
	register("LOLWUT", lolwut, 0, -1, false)
	register("RESET", reset, 0, 0, false)

	// The string point surface. SET's tail is option soup, so the handler
	// validates it.
	register("SET", str.Set, 2, -1, true)
	register("GET", str.Get, 1, 1, true)
	register("GETDEL", str.Getdel, 1, 1, true)
	register("GETEX", str.Getex, 1, -1, true)
	register("GETSET", str.Getset, 2, 2, true)
	register("SETNX", str.Setnx, 2, 2, true)
	register("SETEX", str.Setex, 3, 3, true)
	register("PSETEX", str.Psetex, 3, 3, true)
	register("STRLEN", str.Strlen, 1, 1, true)
	// LCS reads two string keys. Co-located it runs on their shared owner; a
	// split pair takes the F17 route, reading each value on its own owner.
	register("LCS", str.Lcs, 2, -1, true)
	table["LCS"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	table["LCS"].cross = str.LcsCross
	// TYPE spans every keyspace f3 keeps (the string store and all five
	// collection registries), so its handler lives here where every type
	// package is in reach. Single-key EXISTS and DEL below share that reach,
	// and their multi-key fan forms (delShardAll, existsShardAll) span every
	// keyspace too, for the same reason: the fan sub-handlers live here.
	register("TYPE", typeCmd, 1, 1, true)

	// The tier-one multi-key commands: a single key keeps the point path,
	// more keys scatter through the fan-out; MGET and MSET always fan. The
	// sub-command handlers are shard-only ops with no verb.
	mget := registerShard(str.MGetShard)
	mset := registerShard(str.MSetShard)
	del := registerShard(delShardAll)
	exists := registerShard(existsShardAll)
	register("EXISTS", existsCmd, 1, -1, true)
	register("DEL", delCmd, 1, -1, true)
	register("UNLINK", delCmd, 1, -1, true)
	// TOUCH counts how many of its keys exist, spanning every keyspace exactly as
	// EXISTS does, so it shares the same point handler and fan sub-handler. Its
	// only extra contract is refreshing each key's access time for the eviction
	// clock; that bump is a no-op here until the LTM access-tracking slice wires
	// it, and it is not observable in the reply, so the count is the whole answer.
	register("TOUCH", existsCmd, 1, -1, true)
	// The read-only expiry queries and PERSIST span every keyspace, so their
	// handlers live here alongside TYPE and EXISTS: a collection key reads as
	// live with no deadline (-1) rather than the missing-key -2 the set-only
	// path used to give a hash, list, zset, or stream key.
	register("TTL", ttlCmd, 1, 1, true)
	register("PTTL", pttlCmd, 1, 1, true)
	register("EXPIRETIME", expiretimeCmd, 1, 1, true)
	register("PEXPIRETIME", pexpiretimeCmd, 1, 1, true)
	register("PERSIST", persistCmd, 1, 1, true)
	// The EXPIRE family sets a key's deadline. It routes through expireRoute so a
	// collection key gets an honest not-yet answer instead of the string path's
	// "no such key" 0: collections cannot carry a key-level TTL until the per-type
	// header-deadline slice lands (Spec/2064/f3/milestones/
	// M-expiry-generic-key-ttl-plan.md). String keys are fully supported here.
	register("EXPIRE", expireCmd, 2, -1, true)
	register("PEXPIRE", pexpireCmd, 2, -1, true)
	register("EXPIREAT", expireatCmd, 2, -1, true)
	register("PEXPIREAT", pexpireatCmd, 2, -1, true)
	// SORT and its read-only twin span list, set, and zset, so they live here
	// with the other cross-type keyspace verbs. Only the plain numeric/ALPHA and
	// BY-nosort rows are wired; the fan-wave BY-pattern/GET/STORE rows are their
	// own deferred slices (spec 2064/f3/17 section 12).
	register("SORT", sortCmd, 1, -1, true)
	register("SORT_RO", sortRoCmd, 1, -1, true)
	register("MGET", nil, 1, -1, true)
	register("MSET", nil, 2, -1, true)
	registerFan("EXISTS", shard.FanCount, exists, false, false)
	registerFan("TOUCH", shard.FanCount, exists, false, false)
	registerFan("DEL", shard.FanCount, del, false, false)
	registerFan("UNLINK", shard.FanCount, del, false, false)
	registerFan("MGET", shard.FanMGet, mget, false, true)
	registerFan("MSET", shard.FanOK, mset, true, true)

	// INFO scatters keyless to every shard: each answers the fixed-width
	// counter blob and the gather sums the fields and renders the text. The
	// optional section argument is accepted and ignored; there is one section.
	// It goes through infoShardAll so the keys count spans every keyspace, the
	// way DBSIZE does, not just the string store.
	info := registerShard(infoShardAll)
	register("INFO", nil, 0, 1, false)
	registerFan("INFO", shard.FanStats, info, false, true)

	// DBSIZE is the same keyless scatter with the count gather: every shard
	// answers its key count and the sum is the reply.
	dbsize := registerShard(dbsizeShardAll)
	register("DBSIZE", nil, 0, 0, false)
	registerFan("DBSIZE", shard.FanCount, dbsize, false, true)

	// KEYS scatters its match pattern keyless to every shard: each owner walks
	// every keyspace it holds (the string store and the five collection
	// registries), glob-filters, and answers a length-prefixed run of its
	// matches, and the gather concatenates them into one unordered bulk array.
	// It fans even for the single pattern argument, so fanArgs carries the tail.
	keys := registerShard(keysShardAll)
	register("KEYS", nil, 1, 1, false)
	registerFan("KEYS", shard.FanKeys, keys, false, true)
	table["KEYS"].fanArgs = true

	// RANDOMKEY scatters keyless to every shard: each owner draws one uniform
	// key from every keyspace it holds and answers it with its key count, and
	// the gather reservoir-picks across the shards weighted by count, so the
	// draw is uniform over the whole keyspace and returns a key whenever one
	// exists on any shard. An empty keyspace answers the null bulk.
	randomkey := registerShard(randomkeyShardAll)
	register("RANDOMKEY", nil, 0, 0, false)
	registerFan("RANDOMKEY", shard.FanRandom, randomkey, false, true)

	// SCAN scatters its cursor and options keyless to every shard: each owner
	// walks every keyspace it holds, filters by the MATCH glob and the TYPE
	// option, and answers a length-prefixed run of matches, and the gather
	// concatenates them under SCAN's two-element cursor envelope. f3 answers the
	// whole keyspace in one page with a terminal "0" cursor, so COUNT is honored
	// as a hint that bounds nothing and a client's cursor loop makes one pass.
	// The cursor and options are validated once here before the scatter, so a
	// bad cursor answers a single error rather than one per shard.
	scan := registerShard(scanShardAll)
	register("SCAN", nil, 1, -1, false)
	registerFan("SCAN", shard.FanScan, scan, false, true)
	table["SCAN"].fanArgs = true
	table["SCAN"].scanOpts = true

	// FLUSHALL scatters a reset intent to every shard; each owner rebuilds
	// its store empty and the gather answers +OK only after every shard has
	// confirmed, so the flush is a barrier against later commands from the
	// same connection. FLUSHDB is an alias: f3 has a single keyspace (no
	// SELECT), so flushing the db is flushing everything. The optional ASYNC
	// and SYNC tokens are both accepted and both run synchronously for now:
	// the reset is a segment rewind plus a truncate, quick enough that a
	// background reclaim buys nothing yet.
	flush := registerShard(flushShardAll)
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

	// ZRANGESTORE writes a ZRANGE selection into a destination sorted set, the two
	// keys being the destination then the source. Like GEOSEARCHSTORE it routes on
	// the destination and co-locates the whole selection-and-store on one owner,
	// falling back to the F17 hop coordinator when the two keys span shards. The
	// smallest call is destination, source, min, and max.
	register("ZRANGESTORE", zset.Zrangestore, 4, -1, true)
	table["ZRANGESTORE"].crossKeys = func(a [][]byte) [][]byte { return a[:2] }
	table["ZRANGESTORE"].cross = zset.ZrangestoreCross

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
	register("HGETDEL", hash.Hgetdel, 4, -1, true)
	register("HGETEX", hash.Hgetex, 4, -1, true)
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
	// when a key is present, and OBJECT HELP with no key round-robins. ENCODING
	// routes through the stream handler, which reports the stream encoding and
	// then delegates every non-stream key down the chain to the hash handler,
	// then list, then set, then the string store, so one OBJECT verb answers for
	// every type. REFCOUNT is answered generically across every keyspace at this
	// layer, since f3 shares no allocations between keys.
	register("OBJECT", objectCmd, 1, -1, false)
	table["OBJECT"].keyAt = 1

	// MEMORY USAGE key routes on the key after its subcommand token, the same
	// keyAt=1 shape as OBJECT, so it reaches the owning shard. STATS and DOCTOR
	// carry no key and aggregate across every shard: subFan sends them through
	// the keyless fan with the INFO counter blob as the per-shard partial, and
	// the gather renders each one's reply. HELP and any other subcommand fall to
	// the point path and the unknown-subcommand error.
	register("MEMORY", memoryCmd, 1, -1, false)
	table["MEMORY"].keyAt = 1
	table["MEMORY"].fanOp = registerShard(infoShardAll)
	table["MEMORY"].subFan = memorySubFan
}

// memorySubFan routes the keyless MEMORY subcommands. STATS and DOCTOR scatter
// to every shard and fold the per-shard counter blob into their own reply; every
// other form (USAGE with its key, HELP, an unknown token) returns ok=false and
// stays on the point path.
func memorySubFan(args [][]byte) (shard.FanKind, bool) {
	if len(args) != 2 {
		return 0, false
	}
	switch {
	case tokenIs(args[1], "STATS"):
		return shard.FanMemStats, true
	case tokenIs(args[1], "DOCTOR"):
		return shard.FanMemDoctor, true
	}
	return 0, false
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
	if e.subFan != nil {
		if kind, ok := e.subFan(args); ok {
			err := c.DoFanAll(e.fanOp, kind)
			if err == shard.ErrTooBig {
				return oops(c, "ERR command too large")
			}
			return err
		}
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
		if e.scanOpts {
			if _, _, msg := parseScan(args[1:]); msg != "" {
				return oops(c, msg)
			}
		}
		var err error
		if e.fanArgs {
			// A keyless fan that still carries an argument (KEYS hands every shard
			// its match pattern) scatters the tail verbatim to every owner.
			err = c.DoFanAllArgs(e.fanOp, e.fan, args[1:])
		} else {
			err = c.DoFanAll(e.fanOp, e.fan)
		}
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

// selectDB answers SELECT index: f3 keeps a single keyspace with no numbered
// database and no per-connection db state, so it accepts index 0 and refuses any
// other, the honest answer for a server that offers one database. FLUSHDB and
// FLUSHALL are aliases for the same reason.
func selectDB(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, err := strconv.ParseInt(string(args[0]), 10, 64)
	if err != nil {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if n != 0 {
		r.Err("ERR DB index is out of range")
		return
	}
	r.Status("OK")
}

// timeCmd answers TIME: a two element array of the current unix time as seconds
// and the microseconds within that second. f3 reads the per-batch millisecond
// clock, so the microsecond field lands on a millisecond boundary (its low three
// digits are zero), the resolution the cached clock carries.
func timeCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var scratch [24]byte
	out := resp.AppendArrayHeader(cx.Aux[:0], 2)
	out = resp.AppendBulk(out, strconv.AppendInt(scratch[:0], cx.NowMs/1000, 10))
	out = resp.AppendBulk(out, strconv.AppendInt(scratch[:0], cx.NowMs%1000*1000, 10))
	cx.Aux = out
	r.Raw(out)
}

// lolwut answers LOLWUT: in Redis a piece of version art, a cosmetic bulk with
// no contract beyond being human readable. f3 draws no art and returns a one
// line greeting naming the server. Any arguments (the optional VERSION token and
// its number, or anything else) are accepted and ignored, matching Redis, which
// never errors on LOLWUT's tail.
func lolwut(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Bulk([]byte("aki\n"))
}

// reset answers RESET: return the connection to its initial state and reply
// +RESET. Redis resets the selected database, any MULTI, WATCH, subscriptions,
// MONITOR, reply mode, and authentication. f3 offers a single database (SELECT
// only accepts 0), speaks RESP2 with no HELLO, and carries none of that
// per-connection state, so there is nothing to unwind: the honest reset for this
// feature set is the +RESET acknowledgement itself.
func reset(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Status("RESET")
}

// typeCmd answers TYPE key, spanning every keyspace f3 keeps: the string store
// and the five collection registries. A key lives in exactly one of them, so the
// probes are mutually exclusive and their order is only cosmetic; each collection
// probe is the non-creating Has form, so a TYPE against an absent key builds no
// registry and (for streams) registers no maintainer. An absent key reports
// "none", Redis's answer for a key of no type. This supersedes the set-only
// handler the earlier slice registered, which reported "none" for a hash, list,
// zset, or stream key.
func typeCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	switch {
	case cx.St.Exists(key, cx.NowMs):
		r.Status("string")
	case set.Has(cx, key):
		r.Status("set")
	case zset.Has(cx, key):
		r.Status("zset")
	case hash.Has(cx, key):
		r.Status("hash")
	case list.Has(cx, key):
		r.Status("list")
	case stream.Has(cx, key):
		r.Status("stream")
	default:
		r.Status("none")
	}
}

// existsCmd answers the single-key EXISTS point path, spanning every keyspace
// f3 keeps rather than only the string store and the set registry. A key present
// in any one keyspace counts as 1. The multi-key form fans through
// existsShardAll, which spans the same keyspaces, so EXISTS answers alike whether
// it takes one key or many.
func existsCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	if cx.St.Exists(key, cx.NowMs) ||
		set.Has(cx, key) ||
		zset.Has(cx, key) ||
		hash.Has(cx, key) ||
		list.Has(cx, key) ||
		stream.Has(cx, key) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// sharedRefcount is the reference count Redis reports for its interned small
// integer objects (0 through 9999), OBJ_SHARED_REFCOUNT in the server, which is
// INT_MAX. f3 keeps no shared object table, but a string holding a canonical
// integer in that range still reports this sentinel so OBJECT REFCOUNT matches
// Redis byte for byte on the values it would have shared.
const sharedRefcount = 2147483647

// objectCmd answers the OBJECT introspection verb. ENCODING routes down the
// per-type chain (stream then hash then list then set then zset then string) that
// reports each key's storage band. REFCOUNT is answered here across every
// keyspace, since f3 shares no allocation between keys. IDLETIME and FREQ still
// fall to the chain's unknown-subcommand error: both need per-key access metadata
// (an LRU clock or an LFU counter) that no registry keeps yet, so answering them
// would mean inventing a number rather than reading one.
func objectCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if tokenIs(args[0], "REFCOUNT") && len(args) == 2 {
		objectRefcount(cx, args[1], r)
		return
	}
	stream.Object(cx, args, r)
}

// objectRefcount answers OBJECT REFCOUNT key. A present key of any type has a
// single reference, with one exception that keeps parity with Redis: a string
// holding a canonical integer in the shared range 0..9999 reports the shared
// refcount sentinel Redis uses for its interned small integers. A key present in
// no keyspace is the "ERR no such key" Redis returns, which is distinct from the
// null bulk OBJECT ENCODING gives a missing key. The string probe and each Has
// funnel honour the key deadline, so a lazily-expired key reads as no such key.
func objectRefcount(cx *shard.Ctx, key []byte, r shard.Reply) {
	if v, ok := cx.St.GetString(key, cx.NowMs, cx.Val); ok {
		cx.Val = v
		if n, isInt := store.ParseInt(v); isInt && n >= 0 && n < 10000 {
			r.Int(sharedRefcount)
			return
		}
		r.Int(1)
		return
	}
	if set.Has(cx, key) ||
		zset.Has(cx, key) ||
		hash.Has(cx, key) ||
		list.Has(cx, key) ||
		stream.Has(cx, key) {
		r.Int(1)
		return
	}
	r.Err("ERR no such key")
}

// memoryCmd answers the MEMORY introspection verb. USAGE reports an approximate
// resident byte size for one key across every keyspace, an integer for a present
// key and a null for a missing one, matching Redis. The SAMPLES option Redis uses
// to bound nested-element sampling for aggregate types is accepted and ignored,
// since each type's footprint is already an O(1) figure. STATS and DOCTOR are the
// unknown-subcommand error until a later slice wires them.
func memoryCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if tokenIs(args[0], "USAGE") {
		switch {
		case len(args) == 2:
		case len(args) == 4 && tokenIs(args[2], "SAMPLES"):
			if _, err := strconv.Atoi(string(args[3])); err != nil {
				r.Err("ERR value is not an integer or out of range")
				return
			}
		default:
			r.Err("ERR wrong number of arguments for 'memory|usage' command")
			return
		}
		memoryUsage(cx, args[1], r)
		return
	}
	r.Err("ERR Unknown MEMORY subcommand or wrong number of arguments. Try MEMORY HELP.")
}

// memoryUsage answers MEMORY USAGE key: the string store first, then each
// collection keyspace in the OBJECT chain order, so the one key that exists
// anywhere reports its footprint and a key present nowhere is the null Redis
// returns. Each probe is read-only and non-creating, honouring the key deadline
// so a lazily-expired key reads as absent.
func memoryUsage(cx *shard.Ctx, key []byte, r shard.Reply) {
	if n, ok := cx.St.MemoryUsage(key, cx.NowMs); ok {
		r.Int(int64(n))
		return
	}
	if n, ok := set.MemoryUsage(cx, key); ok {
		r.Int(int64(n))
		return
	}
	if n, ok := zset.MemoryUsage(cx, key); ok {
		r.Int(int64(n))
		return
	}
	if n, ok := hash.MemoryUsage(cx, key); ok {
		r.Int(int64(n))
		return
	}
	if n, ok := list.MemoryUsage(cx, key); ok {
		r.Int(int64(n))
		return
	}
	if n, ok := stream.MemoryUsage(cx, key); ok {
		r.Int(int64(n))
		return
	}
	r.Null()
}

// keyDeadline resolves a key's absolute unix-ms expiry across every keyspace,
// the way TTL and its siblings all need it: (-2, _) for an absent key, (-1, _)
// for a live key with no deadline, and (0, ms) for a live key that expires at
// ms. The string store carries the only key-level deadlines f3 keeps today, so
// it answers first; a key absent from the store but present in any collection
// registry is live with no deadline. This spans all types where the earlier
// set-only helper reported a hash, list, zset, or stream key as missing.
func keyDeadline(cx *shard.Ctx, key []byte) (state int, at int64) {
	if at, ok := cx.St.Deadline(key, cx.NowMs); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	// Every collection type now carries its own inline key-level deadline, so ask
	// each directly; at==0 means a live key with no TTL. A key absent from all of
	// them and from the store falls through to -2.
	if at, ok := set.Deadline(cx, key); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	if at, ok := zset.Deadline(cx, key); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	if at, ok := hash.Deadline(cx, key); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	if at, ok := list.Deadline(cx, key); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	if at, ok := stream.Deadline(cx, key); ok {
		if at == 0 {
			return -1, 0
		}
		return 0, at
	}
	return -2, 0
}

// ttlCmd answers TTL key: the remaining lifetime in whole seconds, -2 for a
// missing key, -1 for a key with no deadline. Seconds round to nearest, Redis's
// (ttl+500)/1000.
func ttlCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int((at - cx.NowMs + 500) / 1000)
}

// pttlCmd answers PTTL key: the remaining lifetime in milliseconds, with the
// same -2 and -1 sentinels as TTL.
func pttlCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at - cx.NowMs)
}

// expiretimeCmd answers EXPIRETIME key: the absolute unix time in seconds at
// which the key expires, with the same -2 and -1 sentinels. Seconds floor the ms
// deadline, Redis's expire/1000.
func expiretimeCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at / 1000)
}

// pexpiretimeCmd answers PEXPIRETIME key: the absolute unix time in milliseconds
// at which the key expires, with the same -2 and -1 sentinels.
func pexpiretimeCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at)
}

// persistCmd answers PERSIST key: remove the key's deadline, replying 1 when one
// was removed and 0 otherwise. Every keyspace now carries a key-level deadline, so
// each is asked in turn; an absent key reaches no branch and reads a correct 0.
// hash.Persist clears only the key-level deadline, not the per-field HEXPIRE TTLs
// (those are HPERSIST's job).
func persistCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if cx.St.Persist(args[0], cx.NowMs) || set.Persist(cx, args[0]) || zset.Persist(cx, args[0]) || hash.Persist(cx, args[0]) || list.Persist(cx, args[0]) || stream.Persist(cx, args[0]) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// expireRoute answers an EXPIRE-family command across every keyspace. Each
// collection type now sets its own inline deadline through its backend, and a
// string key falls to str.Expire; the interim not-yet error the earlier expiry
// slices carried is gone now that the stream, the last type, has a deadline. This
// is the plain keyed dispatch the rollout plan converged on
// (M-expiry-generic-key-ttl-plan.md).
func expireRoute(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	key := args[0]
	if set.Has(cx, key) {
		set.Expire(cx, args, r, verb)
		return
	}
	if zset.Has(cx, key) {
		zset.Expire(cx, args, r, verb)
		return
	}
	if hash.Has(cx, key) {
		hash.Expire(cx, args, r, verb)
		return
	}
	if list.Has(cx, key) {
		list.Expire(cx, args, r, verb)
		return
	}
	if stream.Has(cx, key) {
		stream.Expire(cx, args, r, verb)
		return
	}
	str.Expire(cx, args, r, verb)
}

func expireCmd(cx *shard.Ctx, args [][]byte, r shard.Reply)   { expireRoute(cx, args, r, "EXPIRE") }
func pexpireCmd(cx *shard.Ctx, args [][]byte, r shard.Reply)  { expireRoute(cx, args, r, "PEXPIRE") }
func expireatCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) { expireRoute(cx, args, r, "EXPIREAT") }
func pexpireatCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireRoute(cx, args, r, "PEXPIREAT")
}

const sortWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// sortCmd answers SORT key [LIMIT off count] [ASC|DESC] [ALPHA] [BY nosort] over
// a list, set, or sorted set, the plain numeric-or-ALPHA row of spec 2064/f3/17
// section 12. The source is materialized into the one sanctioned sort buffer on
// its owner, sorted, then LIMIT-windowed. It spans every collection type the way
// TYPE and EXISTS do, which is why it lives here. sortRoCmd is the read-only twin.
//
// The fan-wave rows are deferred to their own slices per the spec's split: a BY
// pattern with a '*' dereferences keys on arbitrary owners, GET projects through
// pattern keys, and STORE writes a list to a destination owner; each rides the
// F17 fan the plain row does not need. They report a clear not-yet error rather
// than silently ignoring the option. BY with no '*' is the nosort case and is
// honored here (the source streams in stored order, sorting skipped).
func sortCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sortRun(cx, args, r, false)
}

// sortRoCmd answers SORT_RO, the read-only twin that exists for replica routing.
// It shares the plain-sort core; STORE is not part of its grammar, so a STORE
// token is a syntax error rather than the not-yet error SORT gives.
func sortRoCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sortRun(cx, args, r, true)
}

func sortRun(cx *shard.Ctx, args [][]byte, r shard.Reply, ro bool) {
	key := args[0]
	var (
		desc   bool
		alpha  bool
		nosort bool
		hasLim bool
		off    int
		count  int
	)
	for i := 1; i < len(args); {
		switch {
		case tokenIs(args[i], "ASC"):
			desc = false
			i++
		case tokenIs(args[i], "DESC"):
			desc = true
			i++
		case tokenIs(args[i], "ALPHA"):
			alpha = true
			i++
		case tokenIs(args[i], "LIMIT"):
			if i+2 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			o, err1 := strconv.Atoi(string(args[i+1]))
			c, err2 := strconv.Atoi(string(args[i+2]))
			if err1 != nil || err2 != nil {
				r.Err("ERR value is not an integer or out of range")
				return
			}
			hasLim, off, count = true, o, c
			i += 3
		case tokenIs(args[i], "BY"):
			if i+1 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			// A BY pattern with no '*' cannot name a key, so Redis reads it as the
			// nosort signal: return the source in stored order. A pattern with '*'
			// dereferences arbitrary keys and rides the deferred fan-wave slice.
			if hasStar(args[i+1]) {
				r.Err("ERR SORT BY with a key pattern is not yet supported")
				return
			}
			nosort = true
			i += 2
		case tokenIs(args[i], "GET"):
			r.Err("ERR SORT GET is not yet supported")
			return
		case tokenIs(args[i], "STORE"):
			if ro {
				r.Err("ERR syntax error")
				return
			}
			r.Err("ERR SORT STORE is not yet supported")
			return
		default:
			r.Err("ERR syntax error")
			return
		}
	}

	var elems [][]byte
	switch {
	case list.Has(cx, key):
		elems = list.SortElements(cx, key)
	case set.Has(cx, key):
		elems = set.SortElements(cx, key)
	case zset.Has(cx, key):
		elems = zset.SortElements(cx, key)
	case cx.St.Exists(key, cx.NowMs), hash.Has(cx, key), stream.Has(cx, key):
		r.Err(sortWrongType)
		return
	}

	if !nosort && len(elems) > 1 {
		if alpha {
			sort.SliceStable(elems, func(i, j int) bool {
				return bytesLess(elems[i], elems[j], desc)
			})
		} else {
			scores := make([]float64, len(elems))
			for i, e := range elems {
				f, err := strconv.ParseFloat(string(e), 64)
				if err != nil {
					r.Err("ERR One or more scores can't be converted into double")
					return
				}
				scores[i] = f
			}
			idx := make([]int, len(elems))
			for i := range idx {
				idx[i] = i
			}
			sort.SliceStable(idx, func(a, b int) bool {
				if desc {
					return scores[idx[a]] > scores[idx[b]]
				}
				return scores[idx[a]] < scores[idx[b]]
			})
			sorted := make([][]byte, len(elems))
			for i, j := range idx {
				sorted[i] = elems[j]
			}
			elems = sorted
		}
	}

	if hasLim {
		n := len(elems)
		start := off
		if start < 0 {
			start = 0
		}
		if start > n {
			start = n
		}
		end := n
		if count >= 0 {
			end = start + count
			if end > n {
				end = n
			}
		}
		elems = elems[start:end]
	}

	out := resp.AppendArrayHeader(cx.Aux[:0], len(elems))
	for _, e := range elems {
		out = resp.AppendBulk(out, e)
	}
	cx.Aux = out
	r.Raw(out)
}

// hasStar reports whether a SORT BY/GET pattern contains the '*' substitution
// mark. A pattern without it names no key, the nosort (BY) or constant (GET) case.
func hasStar(p []byte) bool {
	for _, c := range p {
		if c == '*' {
			return true
		}
	}
	return false
}

// bytesLess orders two elements lexicographically for ALPHA sort, reversed when
// desc is set.
func bytesLess(a, b []byte, desc bool) bool {
	c := bytes.Compare(a, b)
	if desc {
		return c > 0
	}
	return c < 0
}

// delCmd answers the single-key DEL and UNLINK point path, spanning every
// keyspace f3 keeps rather than only the string store and the set registry. A
// key lives in exactly one keyspace, so at most one arm removes it; the reply is
// 1 when any did and 0 otherwise. Reclamation is owner-local and immediate, so
// UNLINK shares the path. The multi-key form fans through delShardAll, which
// spans the same keyspaces, so DEL removes alike whether it takes one key or many.
func delCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	removed := cx.St.Del(key, cx.NowMs)
	if set.Delete(cx, key) {
		removed = true
	}
	if zset.Delete(cx, key) {
		removed = true
	}
	if hash.Delete(cx, key) {
		removed = true
	}
	if list.Delete(cx, key) {
		removed = true
	}
	if stream.Delete(cx, key) {
		removed = true
	}
	if removed {
		r.Int(1)
		return
	}
	r.Int(0)
}

// existsShardAll answers an EXISTS sub-command over every keyspace, the fan
// counterpart of existsCmd: it counts each key argument that lives in the string
// store or any collection registry, duplicates included, which is the Redis
// EXISTS contract. It lives here rather than in str for the same reason the
// point handlers do, so every type package is in reach; it supersedes the
// string-only str.ExistsShard the earlier slice fanned, which left a hash, list,
// zset, or stream key invisible to a two-or-more-key EXISTS. Duplicate keys hash
// to one shard, so per-shard counting composes exactly.
func existsShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var n int64
	for _, key := range args {
		if cx.St.Exists(key, cx.NowMs) ||
			set.Has(cx, key) ||
			zset.Has(cx, key) ||
			hash.Has(cx, key) ||
			list.Has(cx, key) ||
			stream.Has(cx, key) {
			n++
		}
	}
	r.FanCount(n)
}

// delShardAll answers a DEL or UNLINK sub-command over every keyspace, the fan
// counterpart of delCmd: for each key it removes the key from whichever keyspace
// holds it and counts the ones that were present. A key lives in exactly one
// keyspace, so at most one arm removes any key. It supersedes the string-only
// str.DelShard the earlier slice fanned, which left a collection key in place on
// a two-or-more-key DEL. UNLINK shares the handler because reclamation is already
// owner-local and immediate. Cold chunks a demoted collection left behind are not
// reclaimed yet, the same deferral the point path carries.
func delShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var n int64
	for _, key := range args {
		removed := cx.St.Del(key, cx.NowMs)
		if set.Delete(cx, key) {
			removed = true
		}
		if zset.Delete(cx, key) {
			removed = true
		}
		if hash.Delete(cx, key) {
			removed = true
		}
		if list.Delete(cx, key) {
			removed = true
		}
		if stream.Delete(cx, key) {
			removed = true
		}
		if removed {
			n++
		}
	}
	r.FanCount(n)
}

// flushShardAll answers a FLUSHALL or FLUSHDB sub-command over every keyspace: it
// resets the shard's string store and then clears each collection registry, so a
// flush empties every type f3 keeps rather than only the string store. Before
// this the sub-handler reset the store alone, so a set, zset, hash, list, or
// stream key survived a flush and DBSIZE and the resident totals stayed wrong. It
// lives here for the same reason the other fan handlers do, where every type
// package is in reach. The reply is the FanOK empty partial; the gather answers
// +OK only once every shard has flushed, so a command pipelined after the flush
// always sees the empty keyspace. Parked blocking-pop and blocking-XREAD clients
// are left blocked, as in Redis, since the list and stream arms keep their waiter
// lists.
func flushShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	cx.St.Reset()
	set.Flush(cx)
	zset.Flush(cx)
	hash.Flush(cx)
	list.Flush(cx)
	stream.Flush(cx)
	r.FanOK()
}

// dbsizeShardAll answers a DBSIZE sub-command over every keyspace: one shard's
// live key count summed across the string store and all five collection
// registries, as a FanCount partial the gather sums into the single integer
// reply. Before this the sub-handler counted the string store alone, so a set,
// zset, hash, list, or stream key was invisible to DBSIZE. It stays O(shards),
// never a scan: each arm reads its registry's map size. It lives here where every
// type package is in reach, the same as the other cross-keyspace fan handlers.
func dbsizeShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n := int64(cx.St.Len())
	n += int64(set.Len(cx))
	n += int64(zset.Len(cx))
	n += int64(hash.Len(cx))
	n += int64(list.Len(cx))
	n += int64(stream.Len(cx))
	r.FanCount(n)
}

// infoShardAll answers one shard's INFO counter blob with the keys field folded
// across every keyspace, not just the string store. str.WriteInfoBlob owns the
// blob layout and knows only its own store, so the collection key counts are
// summed here where every type package is in reach and passed in as the extra,
// mirroring dbsizeShardAll. Before this the keys line counted string keys alone,
// so a set, zset, hash, list, or stream key was missing from the INFO total that
// the memory-bar accounting divides resident bytes by.
func infoShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	extra := uint64(set.Len(cx)) + uint64(zset.Len(cx)) + uint64(hash.Len(cx)) +
		uint64(list.Len(cx)) + uint64(stream.Len(cx))
	vol := set.VolatileLen(cx) + zset.VolatileLen(cx) + hash.VolatileLen(cx) +
		list.VolatileLen(cx) + stream.VolatileLen(cx)
	str.WriteInfoBlob(cx, extra, vol, r)
}

// parseScan reads a SCAN tail: args[0] is the cursor, then any of MATCH pattern,
// COUNT count, and TYPE type in any order. It returns the MATCH glob (nil when
// omitted) and the TYPE name (nil when omitted); the cursor value itself is only
// validated, since f3's SCAN answers the whole keyspace in one page regardless
// of the cursor a client replays. msg is a non-empty error string when the tail
// is malformed: a non-numeric cursor is "ERR invalid cursor", every other fault
// (a dangling option, a non-positive COUNT, an unknown keyword) is the syntax
// error Redis reports. The fan validates once with this before scattering, and
// each shard's handler reuses it to recover the match and type filters.
func parseScan(args [][]byte) (match, typ []byte, msg string) {
	if _, err := strconv.ParseUint(string(args[0]), 10, 64); err != nil {
		return nil, nil, "ERR invalid cursor"
	}
	for i := 1; i < len(args); i++ {
		switch {
		case tokenIs(args[i], "MATCH"):
			if i+1 >= len(args) {
				return nil, nil, "ERR syntax error"
			}
			match = args[i+1]
			i++
		case tokenIs(args[i], "COUNT"):
			if i+1 >= len(args) {
				return nil, nil, "ERR syntax error"
			}
			if n, err := strconv.Atoi(string(args[i+1])); err != nil || n < 1 {
				return nil, nil, "ERR syntax error"
			}
			i++
		case tokenIs(args[i], "TYPE"):
			if i+1 >= len(args) {
				return nil, nil, "ERR syntax error"
			}
			typ = args[i+1]
			i++
		default:
			return nil, nil, "ERR syntax error"
		}
	}
	return match, typ, ""
}

// scanShardAll answers a SCAN sub-command: it walks every keyspace this shard
// holds, the string store and the five collection registries, keeps the keys
// matching the MATCH glob, and, when a TYPE option is present, restricts the
// walk to the one keyspace of that type. It appends the matches as the same
// length-prefixed run KEYS uses, and the FanScan gather concatenates every
// shard's run under SCAN's cursor envelope. The cursor and options were
// validated in dispatch before the scatter, so the parse here cannot fail and
// its error return is ignored. Like KEYS the walk answers the whole keyspace in
// one page; COUNT bounds nothing. Each keyspace iterates through its read-only
// RangeKeys, so the walk sets no residency state, and a cold key is copied into
// the partial as it is seen, before the walk moves past the cold scratch.
func scanShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	match, typ, _ := parseScan(args)
	var part []byte
	emit := func(key []byte) bool {
		if match == nil || globMatch(match, key) {
			part = shard.AppendFanValue(part, key, true)
		}
		return true
	}
	// tokenIs compares case-insensitively against an uppercase word, so the type
	// names are spelled uppercase here though Redis's canonical names and most
	// clients are lowercase.
	if typ == nil || tokenIs(typ, "STRING") {
		cx.St.RangeKeys(cx.NowMs, emit)
	}
	if typ == nil || tokenIs(typ, "SET") {
		set.RangeKeys(cx, emit)
	}
	if typ == nil || tokenIs(typ, "ZSET") {
		zset.RangeKeys(cx, emit)
	}
	if typ == nil || tokenIs(typ, "HASH") {
		hash.RangeKeys(cx, emit)
	}
	if typ == nil || tokenIs(typ, "LIST") {
		list.RangeKeys(cx, emit)
	}
	if typ == nil || tokenIs(typ, "STREAM") {
		stream.RangeKeys(cx, emit)
	}
	r.Raw(part)
}

// keysShardAll answers a KEYS sub-command: it walks every keyspace this shard
// holds, the string store and the five collection registries, glob-filters each
// key against the pattern in args[0], and appends the matches as a length-
// prefixed run the gather concatenates into one bulk array. It lives here where
// every type package is in reach, the same as the other cross-keyspace fan
// handlers, and iterates each keyspace through its read-only RangeKeys so the
// walk sets no residency state. The string store skips an expired resident key
// against the batch clock; the collection keys carry no key-level deadline.
// KEYS is unbounded (Redis's own caveat is not to run it on a live keyspace), so
// the walk never stops early and the partial holds every match. A cold key's
// bytes alias the store's cold scratch only until the next cold read, so each
// match is copied into the partial as it is seen, before the walk moves on.
func keysShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	pattern := args[0]
	var part []byte
	emit := func(key []byte) bool {
		if globMatch(pattern, key) {
			part = shard.AppendFanValue(part, key, true)
		}
		return true
	}
	cx.St.RangeKeys(cx.NowMs, emit)
	set.RangeKeys(cx, emit)
	zset.RangeKeys(cx, emit)
	hash.RangeKeys(cx, emit)
	list.RangeKeys(cx, emit)
	stream.RangeKeys(cx, emit)
	r.Raw(part)
}

// randomkeyShardAll answers a RANDOMKEY sub-command: it reservoir-samples one
// key uniformly across every keyspace this shard holds, the string store and the
// five collection registries, and answers an 8-byte little-endian key count
// followed by the drawn candidate. The gather weights each shard's candidate by
// that count, so the whole-keyspace draw is uniform and returns a key whenever
// any shard has one. A shard with no keys answers a zero count and no candidate.
// It walks each keyspace through its read-only RangeKeys, so the draw sets no
// residency state; a cold candidate's bytes are copied into the reservoir as
// they are seen, before the walk moves past the cold scratch.
func randomkeyShardAll(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var chosen []byte
	var count int64
	pick := func(key []byte) bool {
		count++
		// Reservoir: the nth key seen becomes the held candidate with
		// probability 1/n, so the candidate is uniform over this shard's keys.
		if rand.Int64N(count) == 0 {
			chosen = append(chosen[:0], key...)
		}
		return true
	}
	cx.St.RangeKeys(cx.NowMs, pick)
	set.RangeKeys(cx, pick)
	zset.RangeKeys(cx, pick)
	hash.RangeKeys(cx, pick)
	list.RangeKeys(cx, pick)
	stream.RangeKeys(cx, pick)
	var head [8]byte
	binary.LittleEndian.PutUint64(head[:], uint64(count))
	r.Raw(append(head[:], chosen...))
}

// globMatch reports whether str matches the glob pattern, the same operators
// Redis's stringmatchlen implements: * any run, ? one byte, [...] a class with
// ranges and a leading ^ negation, and \ escaping the next byte. Byte-oriented
// and case sensitive, matching KEYS. It mirrors the per-type SCAN copies
// (zset/scan.go and its siblings) rather than sharing a package, keeping the
// keyspace walk independent of any one type's band.
func globMatch(pattern, str []byte) bool {
	p, sIdx := 0, 0
	for p < len(pattern) {
		switch pattern[p] {
		case '*':
			for p+1 < len(pattern) && pattern[p+1] == '*' {
				p++
			}
			if p+1 == len(pattern) {
				return true
			}
			for i := sIdx; i <= len(str); i++ {
				if globMatch(pattern[p+1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if sIdx == len(str) {
				return false
			}
			sIdx++
			p++
		case '[':
			if sIdx == len(str) {
				return false
			}
			p++
			neg := false
			if p < len(pattern) && pattern[p] == '^' {
				neg = true
				p++
			}
			match := false
			for p < len(pattern) && pattern[p] != ']' {
				if pattern[p] == '\\' && p+1 < len(pattern) {
					p++
					if pattern[p] == str[sIdx] {
						match = true
					}
				} else if p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']' {
					lo, hi := pattern[p], pattern[p+2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if str[sIdx] >= lo && str[sIdx] <= hi {
						match = true
					}
					p += 2
				} else if pattern[p] == str[sIdx] {
					match = true
				}
				p++
			}
			if p < len(pattern) {
				p++ // consume ']'
			}
			if match == neg {
				return false
			}
			sIdx++
		case '\\':
			if p+1 < len(pattern) {
				p++
			}
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		default:
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		}
	}
	return sIdx == len(str)
}
