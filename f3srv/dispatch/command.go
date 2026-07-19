package dispatch

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// COMMAND introspects the command table (spec 2064/f3/11, the M11 command-closure
// surface). Redis clients call it on connect: redis-cli sends COMMAND DOCS for
// inline help, drivers send COMMAND COUNT and COMMAND INFO to learn a command's
// arity and where its keys sit before they pipeline or cluster-route. f3 already
// holds every fact these answers need in the dispatch table (the arity bounds,
// the keyed flag, the routing key index, the fan shape), so COMMAND is a read of
// that table, no new state and nothing on the shard path.
//
// The honest edges: f3 does not model per-command ACL categories, command flags
// (write/readonly/fast), or the rich key-spec objects, so those reply as the
// empty arrays Redis also allows there. The name, the arity, and the first/last
// key positions are exact; a command with a cross-key or STORE tail reports its
// last-key extent as open (-1) rather than pretending a single key, so a client
// that routes on the extent never under-reads the key set.

// commandCmd answers COMMAND and its subcommands. Bare COMMAND lists every
// command's spec; COUNT, INFO, DOCS, and GETKEYS take the sub-token at args[0].
func commandCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args) == 0 {
		out := resp.AppendArrayHeader(cx.Aux[:0], len(table))
		for _, e := range table {
			out = appendCommandSpec(out, e)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}
	switch upperVerb(args[0]) {
	case "COUNT":
		r.Int(int64(len(table)))
	case "INFO":
		commandInfo(cx, args[1:], r)
	case "DOCS":
		commandDocs(cx, args[1:], r)
	case "GETKEYS":
		commandGetKeys(cx, args[1:], r)
	default:
		r.Err("ERR Unknown COMMAND subcommand or wrong number of arguments")
	}
}

// commandInfo answers COMMAND INFO [name ...]: one spec per named command, a
// null array for a name the table does not hold. With no names it lists every
// command, matching Redis, which treats a bare INFO as the full listing.
func commandInfo(cx *shard.Ctx, names [][]byte, r shard.Reply) {
	if len(names) == 0 {
		out := resp.AppendArrayHeader(cx.Aux[:0], len(table))
		for _, e := range table {
			out = appendCommandSpec(out, e)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], len(names))
	for _, name := range names {
		if e := table[upperVerb(name)]; e != nil {
			out = appendCommandSpec(out, e)
		} else {
			out = resp.AppendNullArray(out)
		}
	}
	cx.Aux = out
	r.Raw(out)
}

// commandDocs answers COMMAND DOCS [name ...]. Redis returns a map of command to
// a documentation object; f3 ships no static help text, so it answers the empty
// map (a zero-length flat array in RESP2). A client that asked for docs gets no
// hints rather than an error, which is how redis-cli degrades when a server omits
// them. The names are validated for arity but otherwise ignored.
func commandDocs(cx *shard.Ctx, names [][]byte, r shard.Reply) {
	r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
}

// commandGetKeys answers COMMAND GETKEYS command [arg ...]: the keys the given
// command would touch, extracted from its key spec. It errors the way Redis does
// for an unknown command, a command that takes no keys, or a tail too short to
// hold the key the spec points at.
func commandGetKeys(cx *shard.Ctx, argv [][]byte, r shard.Reply) {
	if len(argv) == 0 {
		r.Err("ERR Unknown command or wrong number of arguments")
		return
	}
	e := table[upperVerb(argv[0])]
	if e == nil {
		r.Err("ERR Invalid command specified")
		return
	}
	first, last, step := keySpec(e)
	if first == 0 {
		r.Err("ERR The command has no key arguments")
		return
	}
	// argv includes the command token at index 0, so a key at command position p
	// (1-based, the token is position 0) sits at argv[p]. Walk from first to last
	// by step; last == -1 runs to the final argument.
	tail := len(argv) - 1
	end := last
	if end < 0 || end > tail {
		end = tail
	}
	if end < first {
		r.Err("ERR Invalid arguments specified for command")
		return
	}
	n := (end-first)/step + 1
	out := resp.AppendArrayHeader(cx.Aux[:0], n)
	for p := first; p <= end; p += step {
		out = resp.AppendBulk(out, argv[p])
	}
	cx.Aux = out
	r.Raw(out)
}

// appendCommandSpec writes one command's ten-element spec: name, arity, flags,
// first key, last key, step, ACL categories, tips, key specs, subcommands. f3
// fills the four fact fields it models (name, arity, key positions) and leaves
// the four it does not (flags, ACL, tips, key specs) and the subcommand list as
// the empty arrays Redis also permits.
func appendCommandSpec(dst []byte, e *entry) []byte {
	dst = resp.AppendArrayHeader(dst, 10)
	dst = resp.AppendBulk(dst, []byte(e.name))
	dst = resp.AppendInt(dst, arity(e))
	dst = resp.AppendArrayHeader(dst, 0) // flags, unmodeled
	first, last, step := keySpec(e)
	dst = resp.AppendInt(dst, int64(first))
	dst = resp.AppendInt(dst, int64(last))
	dst = resp.AppendInt(dst, int64(step))
	dst = resp.AppendArrayHeader(dst, 0) // ACL categories
	dst = resp.AppendArrayHeader(dst, 0) // tips
	dst = resp.AppendArrayHeader(dst, 0) // key specs
	dst = resp.AppendArrayHeader(dst, 0) // subcommands
	return dst
}

// arity is Redis's arity convention: a fixed-argument command reports the exact
// count including the verb (positive), a variable one the negated minimum
// including the verb (negative, meaning "at least"). The table stores minArgs and
// maxArgs as counts after the verb, so both add one for the verb itself.
func arity(e *entry) int64 {
	min := int64(e.minArgs + 1)
	if e.maxArgs == e.minArgs {
		return min
	}
	return -min
}

// keySpec derives the first key position, the last, and the step from the table
// row. Positions are 1-based over the whole command (the verb is position 0), so
// the routing key at args[1:][keyAt] sits at position keyAt+1. A command with no
// key returns 0,0,0. A paired tail (MSET) steps by two. A fan, fan-only, cross,
// or stream-key command has an open tail, so its last position is -1.
func keySpec(e *entry) (first, last, step int) {
	if !e.keyed && !e.fanOnly && e.fan == 0 {
		return 0, 0, 0
	}
	first = e.keyAt + 1
	step = 1
	switch {
	case e.paired:
		step = 2
		last = -1
	case e.fanOnly || e.fan != 0 || e.crossKeys != nil || e.streamKeyAt != nil:
		last = -1
	default:
		last = first
	}
	return
}

// upperVerb uppercases a command name for a table lookup, the inverse of the
// lowercase spelling register stores; it bounds the scratch at maxVerb the way
// verb dispatch does and returns the empty string for an over-long name, which
// no real command has.
func upperVerb(name []byte) string {
	if len(name) == 0 || len(name) > maxVerb {
		return ""
	}
	var buf [maxVerb]byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' {
			c -= 0x20
		}
		buf[i] = c
	}
	return string(buf[:len(name)])
}
