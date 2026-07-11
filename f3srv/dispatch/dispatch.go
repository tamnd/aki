// Package dispatch is the command table: verb lookup, arity check, and the
// route into the shard runtime, all on the connection's reader goroutine.
// Errors discovered here (unknown verb, wrong arity) still travel through the
// hop as OpError so their replies keep pipeline order.
package dispatch

import (
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/str"
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
	// OBJECT routes by the key after its subcommand token (OBJECT ENCODING
	// key), so it keys on args[1] of the argument tail, not args[0]. Marked
	// keyless here; the keyAt route in Dispatch sends it to the owning shard
	// when a key is present, and OBJECT HELP with no key round-robins.
	register("OBJECT", set.Object, 1, -1, false)
	table["OBJECT"].keyAt = 1
}

// Handlers returns the op-indexed handler vector for Runtime.Use.
func Handlers() []shard.Handler { return handlers }

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
	if e.keyAt > 0 && n > e.keyAt {
		// A verb whose routing key is not its first argument (OBJECT) goes to
		// the shard owning args[keyAt]; without that key it falls through to the
		// keyless path below.
		err := c.DoAt(e.op, e.keyAt, args[1:])
		if err == shard.ErrTooBig {
			return oops(c, "ERR command too large")
		}
		return err
	}
	err := c.Do(e.op, e.keyed, args[1:])
	if err == shard.ErrTooBig {
		// The command never entered a node, so the error reply can take its
		// pipeline slot and the connection lives on.
		return oops(c, "ERR command too large")
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
