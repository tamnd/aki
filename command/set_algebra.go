package command

import (
	"strings"
)

// setAlgebraCommands returns the set algebra commands: the intersection, union
// and difference family with their STORE and CARD variants (doc 10 §9, §10).
func setAlgebraCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "sinter", Group: GroupSet, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOp(ctx, opInter, ctx.Argv[1:]) }},
		{Name: "sunion", Group: GroupSet, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOp(ctx, opUnion, ctx.Argv[1:]) }},
		{Name: "sdiff", Group: GroupSet, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOp(ctx, opDiff, ctx.Argv[1:]) }},
		{Name: "sinterstore", Group: GroupSet, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOpStore(ctx, opInter) }},
		{Name: "sunionstore", Group: GroupSet, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOpStore(ctx, opUnion) }},
		{Name: "sdiffstore", Group: GroupSet, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: func(ctx *Ctx) { handleSetOpStore(ctx, opDiff) }},
		{Name: "sintercard", Group: GroupSet, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleSInterCard},
	}
}

// setOp names the three set algebra operations.
type setOp int

const (
	opInter setOp = iota
	opUnion
	opDiff
)

// handleSetOp (SINTER, SUNION, SDIFF) lives in set_algebra_stream.go, where it
// streams the result without materializing any whole coll source.

// handleSetOpStore (SINTERSTORE, SUNIONSTORE, SDIFFSTORE) lives in
// set_algebra_store_stream.go, where it streams the result into the destination
// without materializing any whole source. When the destination aliases a source it
// streams into a scratch key and installs that onto the destination, so that path
// stays bounded too.

// setStoreEvent names the keyspace event for the STORE form of each set algebra
// operation.
func setStoreEvent(op setOp) string {
	switch op {
	case opInter:
		return "sinterstore"
	case opUnion:
		return "sunionstore"
	default:
		return "sdiffstore"
	}
}

// handleSInterCard implements SINTERCARD numkeys key [key ...] [LIMIT limit]: the
// intersection cardinality without materializing the result.
func handleSInterCard(ctx *Ctx) {
	numkeys, ok := parseInteger(ctx.Argv[1])
	if !ok {
		ctx.enc().WriteError("ERR numkeys should be greater than 0")
		return
	}
	if numkeys <= 0 {
		ctx.enc().WriteError("ERR numkeys can't be non-positive")
		return
	}
	idx := 2 + int(numkeys)
	if idx > len(ctx.Argv) {
		ctx.enc().WriteError("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := ctx.Argv[2:idx]
	limit := 0
	if idx < len(ctx.Argv) {
		if !strings.EqualFold(string(ctx.Argv[idx]), "LIMIT") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		idx++
		if idx >= len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		l, ok := parseInteger(ctx.Argv[idx])
		if !ok || l < 0 {
			ctx.enc().WriteError("ERR LIMIT can't be negative")
			return
		}
		idx++
		if idx != len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		limit = int(l)
	}

	n, wrongTyp, ok := sinterCardBounded(ctx, keys, limit)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// toMembership builds a membership set keyed by member bytes.
func toMembership(members [][]byte) map[string]struct{} {
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[string(m)] = struct{}{}
	}
	return set
}
