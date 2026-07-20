// The data-dependent dereference fan of SORT (spec 2064/f3/17 section 12). A plain
// numeric or ALPHA sort, and the BY-nosort case, run on one owner through the point
// path (sortRun). Three options instead resolve keys the command never named, on
// arbitrary owners, from bytes read out of the source: BY pattern with a '*'
// dereferences one weight key per element, GET pattern projects one value per
// element (GET # is the element itself), and STORE writes the result as a list to a
// destination owner. Those keys are not known at dispatch time, so they cannot ride
// the static crossKeys plan every other tier-two command uses. dispatchSort instead
// arms an intent only on the source (and the STORE destination), runs the body on a
// coordinator goroutine, and the body issues the dereference wave through
// Txn.ReadShard, the lock-free read hop the spec sanctions for a fan that needs a
// consistent epoch but no intent locks. Reads group by owner, one hop per shard.
//
// SORT is a correctness row, never a perf row (doc 17 section 12), so every buffer
// here is a plain allocation off the hot path; the sort buffer holds (weight,
// element) pairs and is the single sanctioned materialization.
package dispatch

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
	"github.com/tamnd/aki/f3srv/resp"
)

// sortOpts is one parsed SORT option set, shared by the point path (sortRun) and
// the coordinator path (sortCross) so both read the grammar identically.
type sortOpts struct {
	desc   bool
	alpha  bool
	nosort bool   // last BY had no '*': return the source in stored order
	byPat  []byte // last BY pattern that carried a '*' (nil otherwise)
	hasLim bool
	off    int
	count  int
	gets   [][]byte // GET patterns in order; "#" kept verbatim
	store  []byte   // STORE destination (nil when absent)
}

// fanning reports whether the options require the dereference coordinator: a BY
// pattern to dereference or any GET to project. STORE is tracked separately since
// it can accompany a plain sort.
func (o *sortOpts) fanning() bool { return o.byPat != nil || len(o.gets) > 0 }

// parseSortOpts walks the SORT option tail (everything after the key) into an
// option set, returning a Redis error text (empty on success). It matches the
// grammar Redis accepts: ASC/DESC/ALPHA flags, LIMIT off count, BY pattern, one or
// more GET patterns, and STORE dest (a syntax error under SORT_RO). A BY pattern
// with no '*' is the nosort signal; the last BY wins.
func parseSortOpts(tail [][]byte, ro bool) (o sortOpts, errMsg string) {
	for i := 0; i < len(tail); {
		switch {
		case tokenIs(tail[i], "ASC"):
			o.desc = false
			i++
		case tokenIs(tail[i], "DESC"):
			o.desc = true
			i++
		case tokenIs(tail[i], "ALPHA"):
			o.alpha = true
			i++
		case tokenIs(tail[i], "LIMIT"):
			if i+2 >= len(tail) {
				return o, "ERR syntax error"
			}
			off, err1 := strconv.Atoi(string(tail[i+1]))
			cnt, err2 := strconv.Atoi(string(tail[i+2]))
			if err1 != nil || err2 != nil {
				return o, "ERR value is not an integer or out of range"
			}
			o.hasLim, o.off, o.count = true, off, cnt
			i += 3
		case tokenIs(tail[i], "BY"):
			if i+1 >= len(tail) {
				return o, "ERR syntax error"
			}
			if hasStar(tail[i+1]) {
				o.byPat, o.nosort = tail[i+1], false
			} else {
				// A BY pattern with no '*' names no key: Redis reads it as the nosort
				// signal and returns the source in stored order.
				o.byPat, o.nosort = nil, true
			}
			i += 2
		case tokenIs(tail[i], "GET"):
			if i+1 >= len(tail) {
				return o, "ERR syntax error"
			}
			o.gets = append(o.gets, tail[i+1])
			i += 2
		case tokenIs(tail[i], "STORE"):
			if ro || i+1 >= len(tail) {
				return o, "ERR syntax error"
			}
			o.store = tail[i+1]
			i += 2
		default:
			return o, "ERR syntax error"
		}
	}
	return o, ""
}

// sortLimit resolves the LIMIT window [start, end) over a result of length n,
// clamping an offset past the end to an empty window and a negative offset to the
// front, the same arithmetic Redis applies. A negative count means "to the end".
func sortLimit(o sortOpts, n int) (start, end int) {
	if !o.hasLim {
		return 0, n
	}
	start = o.off
	if start < 0 {
		start = 0
	}
	if start > n {
		start = n
	}
	end = n
	if o.count >= 0 {
		end = start + o.count
		if end > n {
			end = n
		}
	}
	return start, end
}

// dispatchSort routes one SORT/SORT_RO. It peeks the option shape: a plain sort
// (or a malformed tail) stays on the point path, where one owner materializes its
// collection and sortRun answers the exact parse error in place; a dereference,
// projection, or STORE copies the argument tail and runs on a coordinator holding
// an intent on the source (and the STORE destination), so the body can issue the
// read-only owner hops the fan needs.
func dispatchSort(c *shard.Conn, e *entry, args [][]byte) error {
	fanning, storeIdx := sortShape(args[1:])
	if !fanning && storeIdx < 0 {
		err := c.Do(e.op, e.keyed, args[1:])
		if err == shard.ErrTooBig {
			return oops(c, "ERR command too large")
		}
		return err
	}
	ro := e.sortRO
	a := copyTail(args)
	keys := [][]byte{a[0]}
	if storeIdx >= 0 {
		keys = append(keys, a[storeIdx])
	}
	err := c.DoTxn(keys, func(t *shard.Txn) []byte {
		return sortCross(t, a, ro)
	})
	if err == shard.ErrTooBig {
		return oops(c, "ERR command too large")
	}
	return err
}

// sortShape scans the option tail just enough to choose the route: whether a
// dereference fan is present (a BY pattern with '*', or any GET), and the tail
// index of a STORE destination (-1 when absent). It is deliberately lax: a
// malformed tail it cannot classify falls to the point path, where the full parser
// answers the precise error, so a mis-scan can only route a doomed command to a
// path that still errors correctly, never corrupt a good one.
func sortShape(tail [][]byte) (fanning bool, storeIdx int) {
	storeIdx = -1
	for i := 0; i < len(tail); {
		switch {
		case tokenIs(tail[i], "BY") && i+1 < len(tail):
			if hasStar(tail[i+1]) {
				fanning = true
			}
			i += 2
		case tokenIs(tail[i], "GET") && i+1 < len(tail):
			fanning = true
			i += 2
		case tokenIs(tail[i], "STORE") && i+1 < len(tail):
			storeIdx = i + 1
			i += 2
		case tokenIs(tail[i], "LIMIT"):
			i += 3
		default:
			i++
		}
	}
	return fanning, storeIdx
}

// sortCross runs a dereference/projection/STORE SORT on the coordinator goroutine.
// It reads the source elements on the source owner (intent held), builds the sort
// weights (from BY-pattern keys read across owners, or the elements themselves),
// orders and windows them, projects the GET columns (again across owners), and
// either replies the projection or stores it as a list on the destination owner.
func sortCross(t *shard.Txn, tail [][]byte, ro bool) []byte {
	source := tail[0]
	o, errMsg := parseSortOpts(tail[1:], ro)
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}

	var elems [][]byte
	var wrong bool
	t.Do(source, func(cx *shard.Ctx) {
		switch {
		case list.Has(cx, source):
			elems = list.SortElements(cx, source)
		case set.Has(cx, source):
			elems = set.SortElements(cx, source)
		case zset.Has(cx, source):
			elems = zset.SortElements(cx, source)
		case cx.St.Exists(source, cx.NowMs), hash.Has(cx, source), stream.Has(cx, source):
			wrong = true
		}
	})
	if wrong {
		return resp.AppendError(nil, sortWrongType)
	}

	order := make([]int, len(elems))
	for i := range order {
		order[i] = i
	}
	if !o.nosort && len(elems) > 1 {
		keys := elems
		nilAware := false
		if o.byPat != nil {
			keys = sortDeref(t, o.byPat, elems)
			nilAware = true
		}
		if o.alpha {
			sort.SliceStable(order, func(a, b int) bool {
				return alphaLess(keys[order[a]], keys[order[b]], o.desc, nilAware)
			})
		} else {
			scores, ok := sortScores(keys)
			if !ok {
				return resp.AppendError(nil, "ERR One or more scores can't be converted into double")
			}
			sort.SliceStable(order, func(a, b int) bool {
				if o.desc {
					return scores[order[a]] > scores[order[b]]
				}
				return scores[order[a]] < scores[order[b]]
			})
		}
	}

	start, end := sortLimit(o, len(order))
	window := order[start:end]

	// Build the output rows: without GET, each row is the element; with GET, each
	// row is the projection of every GET pattern in order, flattened. A nil entry
	// marks a missing key or field, replied as a nil bulk and stored as an empty
	// string.
	var out [][]byte
	if len(o.gets) == 0 {
		out = make([][]byte, len(window))
		for i, idx := range window {
			out[i] = elems[idx]
		}
	} else {
		winElems := make([][]byte, len(window))
		for i, idx := range window {
			winElems[i] = elems[idx]
		}
		cols := make([][][]byte, len(o.gets))
		for g, pat := range o.gets {
			cols[g] = sortDeref(t, pat, winElems)
		}
		out = make([][]byte, 0, len(window)*len(o.gets))
		for i := range window {
			for g := range o.gets {
				out = append(out, cols[g][i])
			}
		}
	}

	if o.store != nil {
		return sortStoreReply(t, o.store, out)
	}
	buf := resp.AppendArrayHeader(nil, len(out))
	for _, v := range out {
		if v == nil {
			if t.Resp3() {
				buf = resp.AppendNull3(buf)
			} else {
				buf = resp.AppendNull(buf)
			}
			continue
		}
		buf = resp.AppendBulk(buf, v)
	}
	return buf
}

// sortDeref resolves one dereference key per element from pattern and reads its
// value, across owners in one Txn.ReadShard hop per shard. A "#" pattern yields the
// element itself with no read. A pattern with a '*' substitutes the element into
// the first '*' of the key part and, if the pattern carries a "->field" suffix
// after the '*', reads that hash field instead of a string. A pattern with no '*'
// (and not "#") names no key, so every entry is nil, matching Redis. The returned
// slice is parallel to elems; a nil entry marks a missing key or field, a non-nil
// (possibly empty) entry a present value.
func sortDeref(t *shard.Txn, pattern []byte, elems [][]byte) [][]byte {
	out := make([][]byte, len(elems))
	if len(pattern) == 1 && pattern[0] == '#' {
		copy(out, elems)
		return out
	}
	type dref struct{ key, field []byte }
	drefs := make([]dref, len(elems))
	byShard := map[int][]int{}
	for i, e := range elems {
		key, field, ok := sortDerefKey(pattern, e)
		if !ok {
			continue // no '*': always missing, out[i] stays nil
		}
		drefs[i] = dref{key, field}
		sh := t.Shard(key)
		byShard[sh] = append(byShard[sh], i)
	}
	for sh, idxs := range byShard {
		idxs := idxs
		t.ReadShard(sh, func(cx *shard.Ctx) {
			for _, i := range idxs {
				d := drefs[i]
				if d.field != nil {
					if v, ok := hash.Field(cx, d.key, d.field); ok {
						out[i] = v
					}
					continue
				}
				if v, ok := cx.St.GetString(d.key, cx.NowMs, nil); ok {
					out[i] = nonNilBytes(v)
				}
			}
		})
	}
	return out
}

// sortDerefKey resolves the concrete key (and optional hash field) a SORT
// BY/GET pattern names for one element. ok is false when the pattern carries no
// '*', which Redis treats as an unconditional miss. The element is substituted
// into the first '*' of the key; a "->field" suffix that follows the '*' selects a
// hash field, which is taken literally (never '*'-substituted) and must be
// non-empty, exactly as lookupKeyByPattern resolves it.
func sortDerefKey(pattern, elem []byte) (key, field []byte, ok bool) {
	star := bytes.IndexByte(pattern, '*')
	if star < 0 {
		return nil, nil, false
	}
	rest := pattern[star+1:]
	if j := bytes.Index(rest, []byte("->")); j >= 0 && j+2 < len(rest) {
		arrow := star + 1 + j
		return substFirstStar(pattern[:arrow], elem), pattern[arrow+2:], true
	}
	return substFirstStar(pattern, elem), nil, true
}

// substFirstStar replaces the first '*' of keyPat with elem. keyPat is guaranteed
// to contain a '*' by the caller.
func substFirstStar(keyPat, elem []byte) []byte {
	i := bytes.IndexByte(keyPat, '*')
	out := make([]byte, 0, len(keyPat)-1+len(elem))
	out = append(out, keyPat[:i]...)
	out = append(out, elem...)
	return append(out, keyPat[i+1:]...)
}

// sortScores converts weight bytes to numeric scores for a numeric sort: a nil
// entry (a missing BY key) scores 0, the way Redis leaves an absent weight; a
// present entry that cannot parse as a double fails the whole command, so ok is
// false and the caller answers the "can't be converted into double" error.
func sortScores(keys [][]byte) (scores []float64, ok bool) {
	scores = make([]float64, len(keys))
	for i, k := range keys {
		if k == nil {
			continue
		}
		f, err := strconv.ParseFloat(string(k), 64)
		if err != nil {
			return nil, false
		}
		scores[i] = f
	}
	return scores, true
}

// alphaLess orders two sort keys lexicographically for an ALPHA sort. When the
// keys came from a BY pattern (nilAware), a missing weight sorts before any present
// one and two missing weights are equal, matching Redis's NULL cmpobj rule; a
// sort by the element itself never sees a nil key.
func alphaLess(x, y []byte, desc, nilAware bool) bool {
	var c int
	if nilAware && (x == nil || y == nil) {
		switch {
		case x == nil && y == nil:
			c = 0
		case x == nil:
			c = -1
		default:
			c = 1
		}
	} else {
		c = bytes.Compare(x, y)
	}
	if desc {
		return c > 0
	}
	return c < 0
}

// sortStoreReply writes the sort result as a list to dst on its owner (intent
// held) and replies the stored length. A non-empty result replaces whatever dst
// held with a fresh list and fires sortstore; an empty result deletes dst and
// fires a generic del only when dst existed, the way Redis handles a store that
// produces nothing. A nil projection entry stores as an empty string.
func sortStoreReply(t *shard.Txn, dst []byte, out [][]byte) []byte {
	n := len(out)
	t.Do(dst, func(cx *shard.Ctx) {
		existed := keyExistsAnywhere(cx, dst)
		restoreClear(cx, dst)
		if n > 0 {
			vals := make([][]byte, n)
			for i, v := range out {
				vals[i] = nonNilBytes(v)
			}
			list.SortStore(cx, dst, vals)
			cx.NotifyKeyspaceEvent(shard.NotifyList, "sortstore", dst)
		} else if existed {
			cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", dst)
		}
	})
	return resp.AppendInt(nil, int64(n))
}

// nonNilBytes returns b unchanged when it is non-nil, or an empty non-nil slice
// when it is nil, so a present-but-empty value stays distinct from an absent one
// through the dereference and store paths.
func nonNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
