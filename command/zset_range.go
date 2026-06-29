package command

import (
	"bytes"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// zsetRangeCommands returns the range and range-removal family: ZRANGE and its
// deprecated siblings, the count commands, the range-removal commands, and
// ZRANGESTORE (doc 11 §7.7 through §7.20).
func zsetRangeCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "zrange", Group: GroupSortedSet, Since: "1.2.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRange},
		{Name: "zrevrange", Group: GroupSortedSet, Since: "1.2.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRevRange},
		{Name: "zrangebyscore", Group: GroupSortedSet, Since: "1.0.5",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRangeBy(ctx, true, false, false, true) }},
		{Name: "zrevrangebyscore", Group: GroupSortedSet, Since: "2.2.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRangeBy(ctx, true, false, true, true) }},
		{Name: "zrangebylex", Group: GroupSortedSet, Since: "2.8.9",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRangeBy(ctx, false, true, false, false) }},
		{Name: "zrevrangebylex", Group: GroupSortedSet, Since: "2.8.9",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleZRangeBy(ctx, false, true, true, false) }},
		{Name: "zcount", Group: GroupSortedSet, Since: "2.0.0",
			Arity: 4, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZCount},
		{Name: "zlexcount", Group: GroupSortedSet, Since: "2.8.9",
			Arity: 4, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZLexCount},
		{Name: "zremrangebyrank", Group: GroupSortedSet, Since: "2.0.0",
			Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRemRangeByRank},
		{Name: "zremrangebyscore", Group: GroupSortedSet, Since: "1.2.0",
			Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRemRangeByScore},
		{Name: "zremrangebylex", Group: GroupSortedSet, Since: "2.8.9",
			Arity: 4, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZRemRangeByLex},
		{Name: "zrangestore", Group: GroupSortedSet, Since: "6.2.0",
			Arity: -5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleZRangeStore},
	}
}

// rangeSpec holds the parsed ZRANGE options.
type rangeSpec struct {
	byScore    bool
	byLex      bool
	rev        bool
	limit      bool
	offset     int64
	count      int64
	withScores bool
}

// scoreBound is one endpoint of a score range, inclusive unless excl is set.
type scoreBound struct {
	value float64
	excl  bool
}

// lexBound is one endpoint of a lex range. inf is -1 for the minus-infinity
// bound, +1 for plus-infinity, 0 for a finite member value.
type lexBound struct {
	inf   int
	value []byte
	excl  bool
}

// parseScoreBound parses a score range endpoint, with a leading "(" meaning
// exclusive.
func parseScoreBound(b []byte) (scoreBound, bool) {
	if len(b) == 0 {
		return scoreBound{}, false
	}
	excl := false
	s := b
	if b[0] == '(' {
		excl = true
		s = b[1:]
	}
	v, ok := parseScore(s)
	if !ok {
		return scoreBound{}, false
	}
	return scoreBound{value: v, excl: excl}, true
}

// scoreInRange reports whether score falls within the [lo, hi] bounds.
func scoreInRange(score float64, lo, hi scoreBound) bool {
	if lo.excl {
		if score <= lo.value {
			return false
		}
	} else if score < lo.value {
		return false
	}
	if hi.excl {
		if score >= hi.value {
			return false
		}
	} else if score > hi.value {
		return false
	}
	return true
}

// parseLexBound parses a lex range endpoint: "[m" inclusive, "(m" exclusive,
// "-" the low infinity, "+" the high infinity.
func parseLexBound(b []byte) (lexBound, bool) {
	if len(b) == 0 {
		return lexBound{}, false
	}
	switch b[0] {
	case '-':
		if len(b) == 1 {
			return lexBound{inf: -1}, true
		}
	case '+':
		if len(b) == 1 {
			return lexBound{inf: 1}, true
		}
	case '[':
		return lexBound{value: b[1:]}, true
	case '(':
		return lexBound{value: b[1:], excl: true}, true
	}
	return lexBound{}, false
}

// lexAfterLow reports whether member is at or past the low lex bound.
func lexAfterLow(member []byte, lo lexBound) bool {
	switch lo.inf {
	case -1:
		return true
	case 1:
		return false
	}
	cmp := bytes.Compare(member, lo.value)
	if lo.excl {
		return cmp > 0
	}
	return cmp >= 0
}

// lexBeforeHigh reports whether member is at or before the high lex bound.
func lexBeforeHigh(member []byte, hi lexBound) bool {
	switch hi.inf {
	case 1:
		return true
	case -1:
		return false
	}
	cmp := bytes.Compare(member, hi.value)
	if hi.excl {
		return cmp < 0
	}
	return cmp <= 0
}

// reverseZ returns the pairs in reverse order.
func reverseZ(in []zmember) []zmember {
	out := make([]zmember, len(in))
	for i, zm := range in {
		out[len(in)-1-i] = zm
	}
	return out
}

// applyLimit applies a ZRANGE LIMIT offset/count to an already-directed result.
// A negative offset yields nothing; a negative count keeps everything remaining.
func applyLimit(in []zmember, offset, count int64) []zmember {
	if offset < 0 || offset >= int64(len(in)) {
		return nil
	}
	out := in[offset:]
	if count >= 0 && count < int64(len(out)) {
		out = out[:count]
	}
	return out
}

// computeRange resolves a range request against the sorted members. minArg and
// maxArg are the raw endpoint bytes. It returns the matching pairs in result
// order, or a non-empty error string.
func computeRange(members []zmember, minArg, maxArg []byte, spec rangeSpec) ([]zmember, string) {
	switch {
	case spec.byScore:
		lo, hi := minArg, maxArg
		if spec.rev {
			lo, hi = maxArg, minArg
		}
		loB, ok := parseScoreBound(lo)
		if !ok {
			return nil, "ERR min or max is not a float"
		}
		hiB, ok := parseScoreBound(hi)
		if !ok {
			return nil, "ERR min or max is not a float"
		}
		var out []zmember
		for _, zm := range members {
			if scoreInRange(zm.score, loB, hiB) {
				out = append(out, zm)
			}
		}
		if spec.rev {
			out = reverseZ(out)
		}
		if spec.limit {
			out = applyLimit(out, spec.offset, spec.count)
		}
		return out, ""
	case spec.byLex:
		lo, hi := minArg, maxArg
		if spec.rev {
			lo, hi = maxArg, minArg
		}
		loB, ok := parseLexBound(lo)
		if !ok {
			return nil, "ERR min or max not valid string range item"
		}
		hiB, ok := parseLexBound(hi)
		if !ok {
			return nil, "ERR min or max not valid string range item"
		}
		var out []zmember
		for _, zm := range members {
			if lexAfterLow(zm.member, loB) && lexBeforeHigh(zm.member, hiB) {
				out = append(out, zm)
			}
		}
		if spec.rev {
			out = reverseZ(out)
		}
		if spec.limit {
			out = applyLimit(out, spec.offset, spec.count)
		}
		return out, ""
	default:
		start, ok := parseInteger(minArg)
		if !ok {
			return nil, "ERR value is not an integer or out of range"
		}
		stop, ok := parseInteger(maxArg)
		if !ok {
			return nil, "ERR value is not an integer or out of range"
		}
		seq := members
		if spec.rev {
			seq = reverseZ(members)
		}
		return rankSlice(seq, start, stop), ""
	}
}

// rankSlice returns the inclusive [start, stop] slice of seq, resolving negative
// indices from the end and clamping to bounds.
func rankSlice(seq []zmember, start, stop int64) []zmember {
	n := int64(len(seq))
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return nil
	}
	return seq[start : stop+1]
}

// writeRange writes a range result, honoring WITHSCORES.
func (ctx *Ctx) writeRange(result []zmember, withScores bool) {
	enc := ctx.enc()
	if withScores {
		writeScoredPairs(enc, result)
		return
	}
	enc.WriteArrayLen(len(result))
	for _, zm := range result {
		enc.WriteBulkString(zm.member)
	}
}

// parseZRangeArgs parses the ZRANGE option tokens after key, min and max.
func parseZRangeArgs(opts [][]byte) (rangeSpec, string) {
	var s rangeSpec
	for i := 0; i < len(opts); {
		switch strings.ToUpper(string(opts[i])) {
		case "BYSCORE":
			s.byScore = true
			i++
		case "BYLEX":
			s.byLex = true
			i++
		case "REV":
			s.rev = true
			i++
		case "WITHSCORES":
			s.withScores = true
			i++
		case "LIMIT":
			if i+2 >= len(opts) {
				return s, "ERR syntax error"
			}
			off, ok := parseInteger(opts[i+1])
			if !ok {
				return s, "ERR value is not an integer or out of range"
			}
			cnt, ok := parseInteger(opts[i+2])
			if !ok {
				return s, "ERR value is not an integer or out of range"
			}
			s.limit, s.offset, s.count = true, off, cnt
			i += 3
		default:
			return s, "ERR syntax error"
		}
	}
	if s.byScore && s.byLex {
		return s, "ERR syntax error"
	}
	if s.limit && !s.byScore && !s.byLex {
		return s, "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
	}
	if s.withScores && s.byLex {
		return s, "ERR syntax error"
	}
	return s, ""
}

// handleZRange implements the unified ZRANGE command.
func handleZRange(ctx *Ctx) {
	spec, errStr := parseZRangeArgs(ctx.Argv[4:])
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	ctx.runRange(ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], spec)
}

// handleZRevRange implements ZREVRANGE key start stop [WITHSCORES], the rank-only
// descending range.
func handleZRevRange(ctx *Ctx) {
	spec := rangeSpec{rev: true}
	switch len(ctx.Argv) {
	case 4:
	case 5:
		if !strings.EqualFold(string(ctx.Argv[4]), "WITHSCORES") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		spec.withScores = true
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	ctx.runRange(ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], spec)
}

// handleZRangeBy backs the four deprecated ZRANGEBYSCORE/LEX commands. The
// endpoints are always argv[2] then argv[3]; the rev flag makes computeRange
// treat them as the upper-then-lower order those reverse commands use.
func handleZRangeBy(ctx *Ctx, byScore, byLex, rev, allowWithScores bool) {
	spec := rangeSpec{byScore: byScore, byLex: byLex, rev: rev}
	if errStr := parseLegacyRangeOptions(ctx.Argv[4:], allowWithScores, &spec); errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	ctx.runRange(ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], spec)
}

// parseLegacyRangeOptions parses the [WITHSCORES] [LIMIT offset count] tail the
// deprecated range commands accept.
func parseLegacyRangeOptions(opts [][]byte, allowWithScores bool, s *rangeSpec) string {
	for i := 0; i < len(opts); {
		switch strings.ToUpper(string(opts[i])) {
		case "WITHSCORES":
			if !allowWithScores {
				return "ERR syntax error"
			}
			s.withScores = true
			i++
		case "LIMIT":
			if i+2 >= len(opts) {
				return "ERR syntax error"
			}
			off, ok := parseInteger(opts[i+1])
			if !ok {
				return "ERR value is not an integer or out of range"
			}
			cnt, ok := parseInteger(opts[i+2])
			if !ok {
				return "ERR value is not an integer or out of range"
			}
			s.limit, s.offset, s.count = true, off, cnt
			i += 3
		default:
			return "ERR syntax error"
		}
	}
	return ""
}

// runRange loads the sorted set, computes the range, and writes the reply. It is
// the shared body of ZRANGE and its deprecated siblings.
func (ctx *Ctx) runRange(key, minArg, maxArg []byte, spec rangeSpec) {
	var (
		wrongTyp bool
		result   []zmember
		errStr   string
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := zsetHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		// A coll-form sorted set serves a score range by walking the score-index
		// window directly, so the read stays bounded by the matching rows instead of
		// cloning the whole set. Forward (ZRANGEBYSCORE) walks ascending from the low
		// bound; reverse (ZREVRANGEBYSCORE, arg order key max min) walks descending
		// from the high bound with the backward cursor. The rank form needs
		// order-statistics seeks, so it keeps the materialize path for now.
		if found && hdr.IsColl() && spec.byScore {
			// In the reverse command minArg is the high bound and maxArg the low.
			loArg, hiArg := minArg, maxArg
			if spec.rev {
				loArg, hiArg = maxArg, minArg
			}
			loB, ok := parseScoreBound(loArg)
			if !ok {
				errStr = "ERR min or max is not a float"
				return nil
			}
			hiB, ok := parseScoreBound(hiArg)
			if !ok {
				errStr = "ERR min or max is not a float"
				return nil
			}
			if spec.rev {
				result, err = zsetCollRevRangeByScore(db, key, loB, hiB, spec.limit, spec.offset, spec.count)
			} else {
				result, _, err = zsetCollRangeByScore(db, key, loB, hiB, spec.limit, spec.offset, spec.count, false)
			}
			return err
		}
		// A coll-form lex range walks the member-index rows, which are ordered by
		// member bytes, straight from the low (or high, reversed) bound. ZRANGEBYLEX
		// walks forward, ZREVRANGEBYLEX (arg order key max min) walks backward.
		if found && hdr.IsColl() && spec.byLex {
			loArg, hiArg := minArg, maxArg
			if spec.rev {
				loArg, hiArg = maxArg, minArg
			}
			loB, ok := parseLexBound(loArg)
			if !ok {
				errStr = "ERR min or max not valid string range item"
				return nil
			}
			hiB, ok := parseLexBound(hiArg)
			if !ok {
				errStr = "ERR min or max not valid string range item"
				return nil
			}
			if spec.rev {
				result, err = zsetCollRevRangeByLex(db, key, loB, hiB, spec.limit, spec.offset, spec.count)
			} else {
				result, _, err = zsetCollRangeByLex(db, key, loB, hiB, spec.limit, spec.offset, spec.count, false)
			}
			return err
		}
		members, _, _, err := getZSet(db, key)
		if err != nil {
			return err
		}
		result, errStr = computeRange(members, minArg, maxArg, spec)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	ctx.writeRange(result, spec.withScores)
}

// handleZCount implements ZCOUNT key min max.
func handleZCount(ctx *Ctx) {
	ctx.runRangeCount(ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], rangeSpec{byScore: true})
}

// handleZLexCount implements ZLEXCOUNT key min max.
func handleZLexCount(ctx *Ctx) {
	ctx.runRangeCount(ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], rangeSpec{byLex: true})
}

// runRangeCount computes a range and replies its size.
func (ctx *Ctx) runRangeCount(key, minArg, maxArg []byte, spec rangeSpec) {
	var (
		wrongTyp bool
		n        int64
		errStr   string
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := zsetHeader(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		// ZCOUNT over a coll-form set counts the score-index window in place rather
		// than materializing the whole set to measure it.
		if found && hdr.IsColl() && spec.byScore {
			loB, ok := parseScoreBound(minArg)
			if !ok {
				errStr = "ERR min or max is not a float"
				return nil
			}
			hiB, ok := parseScoreBound(maxArg)
			if !ok {
				errStr = "ERR min or max is not a float"
				return nil
			}
			_, n, err = zsetCollRangeByScore(db, key, loB, hiB, false, 0, 0, true)
			return err
		}
		// ZLEXCOUNT counts the member-index window in place the same way.
		if found && hdr.IsColl() && spec.byLex {
			loB, ok := parseLexBound(minArg)
			if !ok {
				errStr = "ERR min or max not valid string range item"
				return nil
			}
			hiB, ok := parseLexBound(maxArg)
			if !ok {
				errStr = "ERR min or max not valid string range item"
				return nil
			}
			_, n, err = zsetCollRangeByLex(db, key, loB, hiB, false, 0, 0, true)
			return err
		}
		members, _, _, err := getZSet(db, key)
		if err != nil {
			return err
		}
		var result []zmember
		result, errStr = computeRange(members, minArg, maxArg, spec)
		n = int64(len(result))
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleZRemRangeByRank implements ZREMRANGEBYRANK key start stop.
func handleZRemRangeByRank(ctx *Ctx) {
	ctx.runRangeRemove(rangeSpec{}, "zremrangebyrank")
}

// handleZRemRangeByScore implements ZREMRANGEBYSCORE key min max.
func handleZRemRangeByScore(ctx *Ctx) {
	ctx.runRangeRemove(rangeSpec{byScore: true}, "zremrangebyscore")
}

// handleZRemRangeByLex implements ZREMRANGEBYLEX key min max.
func handleZRemRangeByLex(ctx *Ctx) {
	ctx.runRangeRemove(rangeSpec{byLex: true}, "zremrangebylex")
}

// runRangeRemove computes the matching members and removes them, replying the
// removed count and deleting the key when it empties.
func (ctx *Ctx) runRangeRemove(spec rangeSpec, event string) {
	key, minArg, maxArg := ctx.Argv[1], ctx.Argv[2], ctx.Argv[3]
	var (
		wrongTyp bool
		emptied  bool
		removed  int64
		errStr   string
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		members, hdr, found, err := getZSet(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		var matched []zmember
		matched, errStr = computeRange(members, minArg, maxArg, spec)
		if errStr != "" {
			return nil
		}
		if len(matched) == 0 {
			return nil
		}
		drop := make(map[string]struct{}, len(matched))
		for _, zm := range matched {
			drop[string(zm.member)] = struct{}{}
		}
		kept := members[:0]
		for _, zm := range members {
			if _, gone := drop[string(zm.member)]; gone {
				continue
			}
			kept = append(kept, zm)
		}
		removed = int64(len(matched))
		if len(kept) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, zsetEncode(kept), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), kept, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	if removed > 0 {
		ctx.notify(notifyZset, event, key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	ctx.enc().WriteInteger(removed)
}

// handleZRangeStore implements ZRANGESTORE dst src min max [...]. It computes the
// range over the source and stores it at the destination, preserving scores.
func handleZRangeStore(ctx *Ctx) {
	spec, errStr := parseZRangeArgs(ctx.Argv[5:])
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	dst, src, minArg, maxArg := ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], ctx.Argv[4]
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
		rangeErr   string
	)
	done := ctx.update(func(db *keyspace.DB) error {
		// The destination is overwritten, but a non-zset destination is still a
		// WRONGTYPE error, so check its type before computing.
		_, dstHdr, dstFound, err := db.Get(dst)
		if err != nil {
			return err
		}
		if dstFound && dstHdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		members, hdr, found, err := getZSet(db, src)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		var result []zmember
		result, rangeErr = computeRange(members, minArg, maxArg, spec)
		if rangeErr != "" {
			return nil
		}
		n = int64(len(result))
		if len(result) == 0 {
			dstDeleted = dstFound
			_, err := db.Delete(dst)
			return err
		}
		stored := make([]zmember, len(result))
		copy(stored, result)
		zsetSort(stored)
		return db.Set(dst, zsetEncode(stored), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), stored, keyspace.EncListpack), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if rangeErr != "" {
		ctx.enc().WriteError(rangeErr)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, "zrangestore", dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}
