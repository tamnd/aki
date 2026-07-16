package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The multi-key zset algebra surface (spec 2064/f3/12 section 6.12): the read
// forms ZUNION, ZINTER, ZDIFF, ZINTERCARD and the store forms ZUNIONSTORE,
// ZINTERSTORE, ZDIFFSTORE, with full WEIGHTS and AGGREGATE (SUM, MIN, MAX)
// semantics and exact Redis error texts.
//
// Every operand key is read from one shard's registry. The read forms route on
// their first operand (keyAt=1, the numkeys-leading route SINTERCARD uses) and
// the store forms route on the destination (args[0], the first-argument route
// SADD uses); either way a handler resolves every source from its own
// owner-local registry with no cross-shard hop. That holds while a command's
// keys are co-located, which the router does not yet guarantee for keys hashing
// to different shards; a true cross-shard gather rides the F17 intent path (its
// slice 3), and until then multi-key zset algebra assumes co-located operands,
// recorded honestly rather than papered over with machinery this slice does not
// own. This is the same constraint the M1 set algebra already documents.
//
// Regular sets as operands (score 1 per member, section 6.12 line 556) are
// deferred with the cross-type keyspace unification: the zset and set registries
// are still separate keyspaces (reg.go), so a zset command cannot read a set
// without a set-side read hook this slice does not own. A set-typed operand
// therefore reads as a missing (empty) key for now, and the hook lands with the
// keyspace slice that unifies the two registries.
//
// The reply is buffer-then-encode (the set algebra shape): members land in the
// shard value scratch as they are produced, the count falls out, and the array
// header plus the page hand over whole through Reply.Raw. WITHSCORES emits the
// flat alternating member, score form, matching the ZRANGE family in this engine
// (the RESP3 pair-array shape of section 6.14 follows that family's convention).

// algebraSpec is the parsed shape of a weighted algebra command: the operand
// keys, one weight per key (all 1 by default), the aggregate mode, and whether a
// read form asked for WITHSCORES.
type algebraSpec struct {
	keys       [][]byte
	weights    []float64
	mode       aggMode
	withScores bool
}

// parseWeighted reads the numkeys-led tail of ZUNION/ZINTER and their STORE
// forms: numkeys, exactly numkeys keys, then optional WEIGHTS, AGGREGATE, and
// (for the read forms) WITHSCORES in any order. tail begins at numkeys. cmd is
// the lowercase command name for the arity error. The error strings are verbatim
// Redis 8.8.
func parseWeighted(tail [][]byte, cmd string, allowWithScores bool) (*algebraSpec, string) {
	numkeys, ok := parseIndex(tail[0])
	if !ok {
		return nil, errNotInt
	}
	if numkeys < 1 {
		return nil, "ERR at least 1 input key is needed for '" + cmd + "' command"
	}
	if numkeys > len(tail)-1 {
		return nil, "ERR syntax error"
	}
	spec := &algebraSpec{
		keys:    tail[1 : 1+numkeys],
		weights: make([]float64, numkeys),
		mode:    aggSum,
	}
	for i := range spec.weights {
		spec.weights[i] = 1
	}
	seenWeights, seenAgg := false, false
	i := 1 + numkeys
	for i < len(tail) {
		switch {
		case eqFold(tail[i], "WEIGHTS"):
			if seenWeights || i+numkeys >= len(tail) {
				return nil, "ERR syntax error"
			}
			for w := 0; w < numkeys; w++ {
				v, okw := parseScore(tail[i+1+w])
				if !okw {
					return nil, "ERR weight value is not a float"
				}
				spec.weights[w] = v
			}
			seenWeights = true
			i += 1 + numkeys
		case eqFold(tail[i], "AGGREGATE"):
			if seenAgg || i+1 >= len(tail) {
				return nil, "ERR syntax error"
			}
			switch {
			case eqFold(tail[i+1], "SUM"):
				spec.mode = aggSum
			case eqFold(tail[i+1], "MIN"):
				spec.mode = aggMin
			case eqFold(tail[i+1], "MAX"):
				spec.mode = aggMax
			default:
				return nil, "ERR syntax error"
			}
			seenAgg = true
			i += 2
		case allowWithScores && eqFold(tail[i], "WITHSCORES"):
			spec.withScores = true
			i++
		default:
			return nil, "ERR syntax error"
		}
	}
	return spec, ""
}

// parseDiff reads the numkeys-led tail of ZDIFF and ZDIFFSTORE: numkeys, exactly
// numkeys keys, then optional WITHSCORES for the read form. ZDIFF takes neither
// WEIGHTS nor AGGREGATE, so any option other than WITHSCORES is a syntax error.
func parseDiff(tail [][]byte, cmd string, allowWithScores bool) (*algebraSpec, string) {
	numkeys, ok := parseIndex(tail[0])
	if !ok {
		return nil, errNotInt
	}
	if numkeys < 1 {
		return nil, "ERR at least 1 input key is needed for '" + cmd + "' command"
	}
	if numkeys > len(tail)-1 {
		return nil, "ERR syntax error"
	}
	spec := &algebraSpec{keys: tail[1 : 1+numkeys]}
	for i := 1 + numkeys; i < len(tail); i++ {
		if !allowWithScores || !eqFold(tail[i], "WITHSCORES") {
			return nil, "ERR syntax error"
		}
		spec.withScores = true
	}
	return spec, ""
}

// gatherZ resolves every operand key against the local registry, carrying its
// weight. wrong is true when any key holds a string value (WRONGTYPE, checked
// before any write); a missing key resolves to a nil zset, read as empty.
func gatherZ(g *reg, cx *shard.Ctx, spec *algebraSpec) (ops []operand, wrong bool) {
	ops = make([]operand, len(spec.keys))
	for i, k := range spec.keys {
		z, w := g.lookup(cx, k)
		if w {
			return nil, true
		}
		ops[i] = operand{z: z, weight: 1}
		if spec.weights != nil {
			ops[i].weight = spec.weights[i]
		}
	}
	return ops, false
}

// emitScored streams the aggregated result into the shard scratch as a flat
// multi-bulk reply, member then score when withScores, and hands it over whole.
func emitScored(cx *shard.Ctx, r shard.Reply, pairs []scoredMember, withScores bool) {
	page := cx.Val[:0]
	var sc [40]byte
	n := 0
	for _, p := range pairs {
		page = resp.AppendBulk(page, p.member)
		n++
		if withScores {
			page = resp.AppendBulk(page, resp.FormatScore(sc[:0], p.score))
			n++
		}
	}
	cx.Val = page
	out := resp.AppendArrayHeader(cx.Aux[:0], n)
	out = append(out, page...)
	cx.Aux = out
	r.Raw(out)
}

// place installs a freshly built result as the destination key, replacing
// whatever it held and discarding any TTL (a STORE destination is a new object).
// An empty result (nil) deletes the destination. It returns the result
// cardinality, the STORE reply. It runs after the result is fully built off the
// sources, so an aliasing STORE (destination is also a source) needs no clone.
func place(cx *shard.Ctx, g *reg, key []byte, result *zset) int {
	cx.St.Del(key, cx.NowMs)
	if result == nil {
		g.drop(key)
		return 0
	}
	if g.acctOn && g.m[string(key)] != nil {
		g.drop(key)
	}
	g.m[string(key)] = result
	g.note(result)
	return result.card()
}

// Zunion answers ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
// [WITHSCORES]: the weighted union of the sources, score-ordered.
func Zunion(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseWeighted(args, "zunion", true)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitScored(cx, r, union(ops, spec.mode), spec.withScores)
}

// Zinter answers ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
// [WITHSCORES]: the weighted intersection of the sources, score-ordered.
func Zinter(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseWeighted(args, "zinter", true)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitScored(cx, r, intersect(ops, spec.mode), spec.withScores)
}

// Zdiff answers ZDIFF numkeys key [key ...] [WITHSCORES]: the members of the
// first source not in any later source, score-ordered.
func Zdiff(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseDiff(args, "zdiff", true)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitScored(cx, r, diff(ops), spec.withScores)
}

// Zintercard answers ZINTERCARD numkeys key [key ...] [LIMIT limit]: the size of
// the intersection, an integer, with LIMIT capping the count and stopping the
// walk early. LIMIT 0 means unlimited (Redis).
func Zintercard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	numkeys, ok := parseIndex(args[0])
	if !ok {
		r.Err(errNotInt)
		return
	}
	if numkeys < 1 {
		r.Err("ERR at least 1 input key is needed for 'zintercard' command")
		return
	}
	if numkeys > len(args)-1 {
		r.Err("ERR syntax error")
		return
	}
	keys := args[1 : 1+numkeys]
	limit := 0
	for i := 1 + numkeys; i < len(args); {
		if !eqFold(args[i], "LIMIT") || i+1 >= len(args) {
			r.Err("ERR syntax error")
			return
		}
		lv, okl := parseIndex(args[i+1])
		if !okl || lv < 0 {
			// Redis parses the LIMIT value through a ranged reader whose custom
			// message covers a non-integer, a negative, and an overflow alike, so a
			// bad limit is always this text, not the generic not-an-integer error.
			r.Err("ERR LIMIT can't be negative")
			return
		}
		limit = lv
		i += 2
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, &algebraSpec{keys: keys})
	if wrong {
		r.Err(wrongType)
		return
	}
	r.Int(int64(intercard(ops, limit)))
}

// Zunionstore answers ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...]
// [AGGREGATE ...]: union the sources, store the result in destination, reply its
// cardinality. An empty result deletes the destination; a wrong-typed source is
// WRONGTYPE and leaves the destination untouched.
func Zunionstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseWeighted(args[1:], "zunionstore", false)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	r.Int(int64(place(cx, g, args[0], buildDest(union(ops, spec.mode)))))
}

// Zinterstore answers ZINTERSTORE destination numkeys key [key ...] [WEIGHTS ...]
// [AGGREGATE ...]: intersect the sources, store the result, reply its
// cardinality.
func Zinterstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseWeighted(args[1:], "zinterstore", false)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	r.Int(int64(place(cx, g, args[0], buildDest(intersect(ops, spec.mode)))))
}

// Zdiffstore answers ZDIFFSTORE destination numkeys key [key ...]: the members
// of the first source not in any later source, stored in destination, reply the
// cardinality.
func Zdiffstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	spec, errMsg := parseDiff(args[1:], "zdiffstore", false)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	g := registry(cx)
	ops, wrong := gatherZ(g, cx, spec)
	if wrong {
		r.Err(wrongType)
		return
	}
	r.Int(int64(place(cx, g, args[0], buildDest(diff(ops)))))
}
