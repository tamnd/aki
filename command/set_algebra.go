package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
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

// handleSetOp implements SINTER, SUNION and SDIFF: compute over the keys and
// reply the result as a set.
func handleSetOp(ctx *Ctx, op setOp, keys [][]byte) {
	var (
		wrongTyp bool
		result   [][]byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		sets, wt, err := loadSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result = computeSetOp(op, sets)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteSetLen(len(result))
	for _, m := range result {
		enc.WriteBulkString(m)
	}
}

// handleSetOpStore implements SINTERSTORE, SUNIONSTORE and SDIFFSTORE: compute
// over the source keys, store the result at the destination, and reply the
// result cardinality. An empty result deletes the destination.
func handleSetOpStore(ctx *Ctx, op setOp) {
	dst := ctx.Argv[1]
	keys := ctx.Argv[2:]
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		// The destination is overwritten, but a non-set destination is still a
		// WRONGTYPE error, so check its type before computing.
		_, dstHdr, dstFound, err := db.Get(dst)
		if err != nil {
			return err
		}
		if dstFound && dstHdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		sets, wt, err := loadSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result := computeSetOp(op, sets)
		n = int64(len(result))
		if len(result) == 0 {
			dstDeleted = dstFound
			_, err := db.Delete(dst)
			return err
		}
		return db.Set(dst, setEncode(result), keyspace.TypeSet, setEncoding(result, keyspace.EncIntset), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifySet, setStoreEvent(op), dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}

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

	var (
		wrongTyp bool
		n        int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		sets, wt, err := loadSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		n = int64(intersectCount(sets, limit))
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// loadSets reads each key's members in argument order. A missing key contributes
// an empty member slice. The wrongTyp flag is set when any key holds a non-set.
func loadSets(db *keyspace.DB, keys [][]byte) ([][][]byte, bool, error) {
	sets := make([][][]byte, len(keys))
	for i, k := range keys {
		members, hdr, found, err := getSet(db, k)
		if err != nil {
			return nil, false, err
		}
		if found && hdr.Type != keyspace.TypeSet {
			return nil, true, nil
		}
		sets[i] = members
	}
	return sets, false, nil
}

// computeSetOp runs the chosen operation over the loaded sets, preserving the
// member order of the first set for union and difference.
func computeSetOp(op setOp, sets [][][]byte) [][]byte {
	switch op {
	case opInter:
		return intersect(sets)
	case opUnion:
		return union(sets)
	default:
		return difference(sets)
	}
}

// intersect returns the members present in every set. An empty input or any
// empty set yields no members.
func intersect(sets [][][]byte) [][]byte {
	if len(sets) == 0 || len(sets[0]) == 0 {
		return nil
	}
	others := make([]map[string]struct{}, len(sets)-1)
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) == 0 {
			return nil
		}
		others[i-1] = toMembership(sets[i])
	}
	var out [][]byte
	for _, m := range sets[0] {
		inAll := true
		for _, set := range others {
			if _, ok := set[string(m)]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			out = append(out, m)
		}
	}
	return out
}

// intersectCount counts the intersection, stopping early once limit members are
// counted when limit is greater than 0.
func intersectCount(sets [][][]byte, limit int) int {
	got := intersect(sets)
	if limit > 0 && len(got) > limit {
		return limit
	}
	return len(got)
}

// union returns the distinct members across all sets in first-seen order.
func union(sets [][][]byte) [][]byte {
	seen := map[string]struct{}{}
	var out [][]byte
	for _, set := range sets {
		for _, m := range set {
			if _, ok := seen[string(m)]; !ok {
				seen[string(m)] = struct{}{}
				out = append(out, m)
			}
		}
	}
	return out
}

// difference returns the members of the first set not present in any later set.
func difference(sets [][][]byte) [][]byte {
	if len(sets) == 0 {
		return nil
	}
	remove := map[string]struct{}{}
	for i := 1; i < len(sets); i++ {
		for _, m := range sets[i] {
			remove[string(m)] = struct{}{}
		}
	}
	var out [][]byte
	for _, m := range sets[0] {
		if _, ok := remove[string(m)]; !ok {
			out = append(out, m)
		}
	}
	return out
}

// toMembership builds a membership set keyed by member bytes.
func toMembership(members [][]byte) map[string]struct{} {
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[string(m)] = struct{}{}
	}
	return set
}
