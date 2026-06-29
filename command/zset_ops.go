package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// zsetOpCommands returns the multi-key sorted-set commands: the union,
// intersection and difference family with their STORE variants, ZINTERCARD, and
// ZMPOP (doc 11 §7.23 through §7.30, §8).
func zsetOpCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "zunion", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: func(ctx *Ctx) { zSetOpRead(ctx, zopUnion) }},
		{Name: "zinter", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: func(ctx *Ctx) { zSetOpRead(ctx, zopInter) }},
		{Name: "zdiff", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: func(ctx *Ctx) { zSetOpRead(ctx, zopDiff) }},
		{Name: "zunionstore", Group: GroupSortedSet, Since: "2.0.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { zSetOpStore(ctx, zopUnion) }},
		{Name: "zinterstore", Group: GroupSortedSet, Since: "2.0.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { zSetOpStore(ctx, zopInter) }},
		{Name: "zdiffstore", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { zSetOpStore(ctx, zopDiff) }},
		{Name: "zintercard", Group: GroupSortedSet, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleZInterCard},
		{Name: "zmpop", Group: GroupSortedSet, Since: "7.0.0",
			Arity: -4, Flags: FlagWrite, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleZMPop},
	}
}

// zsetOp names the three multi-key set operations.
type zsetOp int

const (
	zopUnion zsetOp = iota
	zopInter
	zopDiff
)

// aggMode names the AGGREGATE score combiner.
type aggMode int

const (
	aggSum aggMode = iota
	aggMin
	aggMax
)

// parseNumkeysAndKeys reads the numkeys count at numIdx and the key names that
// follow, using the set-operation error spellings.
func parseNumkeysAndKeys(argv [][]byte, numIdx int, cmdName string) ([][]byte, int, string) {
	num, ok := parseInteger(argv[numIdx])
	if !ok {
		return nil, 0, "ERR value is not an integer or out of range"
	}
	if num < 1 {
		return nil, 0, "ERR at least 1 input key is needed for '" + cmdName + "' command"
	}
	first := numIdx + 1
	if int(num) > len(argv)-first {
		return nil, 0, "ERR syntax error"
	}
	return argv[first : first+int(num)], first + int(num), ""
}

// parseZSetOpOptions parses the WEIGHTS, AGGREGATE and WITHSCORES tail. Weights
// default to 1 per key and aggregate defaults to SUM.
func parseZSetOpOptions(opts [][]byte, numKeys int, allowWeights, allowWithScores bool) ([]float64, aggMode, bool, string) {
	var weights []float64
	agg := aggSum
	withScores := false
	for i := 0; i < len(opts); {
		switch strings.ToUpper(string(opts[i])) {
		case "WEIGHTS":
			if !allowWeights {
				return nil, agg, false, "ERR syntax error"
			}
			if i+1+numKeys > len(opts) {
				return nil, agg, false, "ERR syntax error"
			}
			weights = make([]float64, numKeys)
			for k := range numKeys {
				w, err := strconv.ParseFloat(string(opts[i+1+k]), 64)
				if err != nil {
					return nil, agg, false, "ERR weight value is not a float"
				}
				weights[k] = w
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if !allowWeights {
				return nil, agg, false, "ERR syntax error"
			}
			if i+1 >= len(opts) {
				return nil, agg, false, "ERR syntax error"
			}
			switch strings.ToUpper(string(opts[i+1])) {
			case "SUM":
				agg = aggSum
			case "MIN":
				agg = aggMin
			case "MAX":
				agg = aggMax
			default:
				return nil, agg, false, "ERR syntax error"
			}
			i += 2
		case "WITHSCORES":
			if !allowWithScores {
				return nil, agg, false, "ERR syntax error"
			}
			withScores = true
			i++
		default:
			return nil, agg, false, "ERR syntax error"
		}
	}
	if weights == nil {
		weights = make([]float64, numKeys)
		for i := range weights {
			weights[i] = 1
		}
	}
	return weights, agg, withScores, ""
}

// weightedScore multiplies a score by its weight, coercing the NaN that 0*inf
// produces to 0, matching Redis.
func weightedScore(w, s float64) float64 {
	v := w * s
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// aggregate combines a running score with another under the chosen mode. A SUM
// that lands on NaN (adding +inf and -inf) is coerced to 0, matching Redis.
func aggregate(target, val float64, agg aggMode) float64 {
	switch agg {
	case aggMin:
		return math.Min(target, val)
	case aggMax:
		return math.Max(target, val)
	default:
		t := target + val
		if math.IsNaN(t) {
			return 0
		}
		return t
	}
}

// loadZSets reads each key's pairs in argument order. A missing key contributes
// an empty slice. The wrongTyp flag is set when any key holds a non-zset.
func loadZSets(db *keyspace.DB, keys [][]byte) ([][]zmember, bool, error) {
	sets := make([][]zmember, len(keys))
	for i, k := range keys {
		members, hdr, found, err := getZSet(db, k)
		if err != nil {
			return nil, false, err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			return nil, true, nil
		}
		sets[i] = members
	}
	return sets, false, nil
}

// scoreMap builds a member to score map for a set.
func scoreMap(set []zmember) map[string]float64 {
	m := make(map[string]float64, len(set))
	for _, zm := range set {
		m[string(zm.member)] = zm.score
	}
	return m
}

// computeZSetOp runs the chosen operation and returns the result in sorted order.
func computeZSetOp(op zsetOp, sets [][]zmember, weights []float64, agg aggMode) []zmember {
	switch op {
	case zopUnion:
		return mapToSorted(zunion(sets, weights, agg))
	case zopInter:
		return mapToSorted(zinter(sets, weights, agg))
	default:
		return zdiff(sets)
	}
}

// zunion accumulates the weighted scores of every member across all sets.
func zunion(sets [][]zmember, weights []float64, agg aggMode) map[string]float64 {
	result := map[string]float64{}
	for i, set := range sets {
		for _, zm := range set {
			val := weightedScore(weights[i], zm.score)
			if cur, ok := result[string(zm.member)]; ok {
				result[string(zm.member)] = aggregate(cur, val, agg)
			} else {
				result[string(zm.member)] = val
			}
		}
	}
	return result
}

// zinter keeps only members present in every set, combining their weighted
// scores. An empty set anywhere makes the intersection empty.
func zinter(sets [][]zmember, weights []float64, agg aggMode) map[string]float64 {
	result := map[string]float64{}
	if len(sets) == 0 || len(sets[0]) == 0 {
		return result
	}
	others := make([]map[string]float64, len(sets))
	for j := 1; j < len(sets); j++ {
		if len(sets[j]) == 0 {
			return result
		}
		others[j] = scoreMap(sets[j])
	}
	for _, zm := range sets[0] {
		val := weightedScore(weights[0], zm.score)
		inAll := true
		for j := 1; j < len(sets); j++ {
			s, ok := others[j][string(zm.member)]
			if !ok {
				inAll = false
				break
			}
			val = aggregate(val, weightedScore(weights[j], s), agg)
		}
		if inAll {
			result[string(zm.member)] = val
		}
	}
	return result
}

// zdiff returns the members of the first set absent from every later set, with
// their first-set scores.
func zdiff(sets [][]zmember) []zmember {
	if len(sets) == 0 {
		return nil
	}
	exclude := map[string]struct{}{}
	for j := 1; j < len(sets); j++ {
		for _, zm := range sets[j] {
			exclude[string(zm.member)] = struct{}{}
		}
	}
	var out []zmember
	for _, zm := range sets[0] {
		if _, ex := exclude[string(zm.member)]; !ex {
			out = append(out, zm)
		}
	}
	zsetSort(out)
	return out
}

// mapToSorted turns a member to score map into a sorted pair slice.
func mapToSorted(result map[string]float64) []zmember {
	out := make([]zmember, 0, len(result))
	for k, v := range result {
		out = append(out, zmember{member: []byte(k), score: v})
	}
	zsetSort(out)
	return out
}

// zSetOpRead backs ZUNION, ZINTER and ZDIFF.
func zSetOpRead(ctx *Ctx, op zsetOp) {
	cmdName := strings.ToLower(string(ctx.Argv[0]))
	keys, restIdx, errStr := parseNumkeysAndKeys(ctx.Argv, 1, cmdName)
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	weights, agg, withScores, errStr := parseZSetOpOptions(ctx.Argv[restIdx:], len(keys), op != zopDiff, true)
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	var (
		wrongTyp bool
		result   []zmember
	)
	if !ctx.view(func(db *keyspace.DB) error {
		sets, wt, err := loadZSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result = computeZSetOp(op, sets, weights, agg)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.writeRange(result, withScores)
}

// zSetOpStore backs ZUNIONSTORE, ZINTERSTORE and ZDIFFSTORE.
func zSetOpStore(ctx *Ctx, op zsetOp) {
	cmdName := strings.ToLower(string(ctx.Argv[0]))
	dst := ctx.Argv[1]
	keys, restIdx, errStr := parseNumkeysAndKeys(ctx.Argv, 2, cmdName)
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	weights, agg, _, errStr := parseZSetOpOptions(ctx.Argv[restIdx:], len(keys), op != zopDiff, false)
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		// Only the source keys are type-checked. The destination is overwritten
		// whatever it held, so a string or list at the destination is replaced
		// rather than rejected, matching Redis.
		sets, wt, err := loadZSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result := computeZSetOp(op, sets, weights, agg)
		n = int64(len(result))
		if len(result) == 0 {
			existed, err := db.Delete(dst)
			dstDeleted = existed
			return err
		}
		return db.Set(dst, zsetEncode(result), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), result, keyspace.EncListpack), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, zSetStoreEvent(op), dst)
		ctx.signalReady(dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}

// zSetStoreEvent maps a store operation to its keyspace event name.
func zSetStoreEvent(op zsetOp) string {
	switch op {
	case zopInter:
		return "zinterstore"
	case zopDiff:
		return "zdiffstore"
	default:
		return "zunionstore"
	}
}

// handleZInterCard implements ZINTERCARD numkeys key [key ...] [LIMIT limit].
func handleZInterCard(ctx *Ctx) {
	numkeys, ok := parseInteger(ctx.Argv[1])
	if !ok || numkeys < 1 {
		ctx.enc().WriteError("ERR numkeys should be greater than 0")
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

	n, wrongTyp, ok := zinterCardBounded(ctx, keys, limit)
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleZMPop implements ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]. It
// pops from the first non-empty key.
func handleZMPop(ctx *Ctx) {
	numkeys, ok := parseInteger(ctx.Argv[1])
	if !ok || numkeys < 1 {
		ctx.enc().WriteError("ERR numkeys should be greater than 0")
		return
	}
	idx := 2 + int(numkeys)
	if idx >= len(ctx.Argv) {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	keys := ctx.Argv[2:idx]
	var fromMax bool
	switch strings.ToUpper(string(ctx.Argv[idx])) {
	case "MIN":
		fromMax = false
	case "MAX":
		fromMax = true
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	idx++
	count := int64(1)
	if idx < len(ctx.Argv) {
		if !strings.EqualFold(string(ctx.Argv[idx]), "COUNT") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		idx++
		if idx >= len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		c, ok := parseInteger(ctx.Argv[idx])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		if c < 1 {
			ctx.enc().WriteError("ERR count should be greater than 0")
			return
		}
		count = c
		idx++
		if idx != len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var (
		poppedKey []byte
		popped    []zmember
		wrongTyp  bool
		emptied   bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		for _, key := range keys {
			hdr, found, err := zsetHeader(db, key)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if hdr.Type != keyspace.TypeZSet {
				wrongTyp = true
				return nil
			}
			popped, emptied, err = zsetPopN(db, key, hdr, count, fromMax, ctx.encLimits())
			if err != nil {
				return err
			}
			if len(popped) == 0 {
				continue
			}
			poppedKey = key
			return nil
		}
		return nil
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if poppedKey != nil {
		event := "zpopmin"
		if fromMax {
			event = "zpopmax"
		}
		ctx.notify(notifyZset, event, poppedKey)
		if emptied {
			ctx.notify(notifyGeneric, "del", poppedKey)
		}
	}
	enc := ctx.enc()
	if poppedKey == nil {
		enc.WriteNullArray()
		return
	}
	enc.WriteArrayLen(2)
	enc.WriteBulkString(poppedKey)
	writeNestedScoredPairs(enc, popped)
}
