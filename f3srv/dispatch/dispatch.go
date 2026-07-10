// Package dispatch is the command table: verb lookup, arity check, and the
// route into the shard runtime, all on the connection's reader goroutine.
// Errors discovered here (unknown verb, wrong arity) still travel through the
// hop as OpError so their replies keep pipeline order.
package dispatch

import (
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

func init() {
	register("PING", ping, 0, 1, false)
	register("ECHO", echo, 1, 1, false)

	// The string point surface. SET's tail is option soup, so the handler
	// validates it; DEL and EXISTS are single-key until the fan-out slice.
	register("SET", str.Set, 2, -1, true)
	register("GET", str.Get, 1, 1, true)
	register("STRLEN", str.Strlen, 1, 1, true)
	register("EXISTS", str.Exists, 1, 1, true)
	register("DEL", str.Del, 1, 1, true)
	register("TYPE", str.Type, 1, 1, true)

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
	err := c.Do(e.op, e.keyed, args[1:])
	if err == shard.ErrTooBig {
		// The command never entered a node, so the error reply can take its
		// pipeline slot and the connection lives on.
		return oops(c, "ERR command too large")
	}
	return err
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
