package list

import (
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The list command surface over the inline band (spec 2064/f3/13 section 5).
// Every handler runs on its shard's owner goroutine, so the registry and every
// list in it are plain single-owner state. Array replies are built in the shard
// scratch (cx.Aux) with the resp emitters and handed over whole through
// Reply.Raw, the same one-pass shape the set and zset replies use. Error texts
// and nil/empty reply forms are Redis's, verified live against redis-server
// 8.8.0.

const (
	errNotInt     = "ERR value is not an integer or out of range"
	errOutOfRange = "ERR index out of range"
	errNoSuchKey  = "ERR no such key"
	errSyntax     = "ERR syntax error"
	errPosCount   = "ERR value is out of range, must be positive"
	errRankZero   = "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"
	errCountNeg   = "ERR COUNT can't be negative"
	errMaxlenNeg  = "ERR MAXLEN can't be negative"
)

// Lpush answers LPUSH key element [element ...]: prepend each element in turn,
// creating the key when absent, and reply the new length.
func Lpush(cx *shard.Ctx, args [][]byte, r shard.Reply) { pushCmd(cx, args, r, true, true) }

// Rpush answers RPUSH key element [element ...]: append each element, creating
// the key when absent, and reply the new length.
func Rpush(cx *shard.Ctx, args [][]byte, r shard.Reply) { pushCmd(cx, args, r, false, true) }

// Lpushx answers LPUSHX key element [element ...]: prepend only when the key
// already holds a list, else reply 0 without creating it.
func Lpushx(cx *shard.Ctx, args [][]byte, r shard.Reply) { pushCmd(cx, args, r, true, false) }

// Rpushx answers RPUSHX key element [element ...]: append only when the key
// already holds a list, else reply 0.
func Rpushx(cx *shard.Ctx, args [][]byte, r shard.Reply) { pushCmd(cx, args, r, false, false) }

func pushCmd(cx *shard.Ctx, args [][]byte, r shard.Reply, front, create bool) {
	g := registry(cx)
	key := args[0]
	l, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if l == nil {
		if !create {
			r.Int(0)
			return
		}
		l = newList()
		g.m[string(key)] = l
	}
	for _, v := range args[1:] {
		if front {
			l.pushFront(v)
		} else {
			l.pushBack(v)
		}
	}
	r.Int(int64(l.length()))
}

// Lpop answers LPOP key [count]; Rpop answers RPOP key [count]. Without a count
// the reply is a bulk string (nil when the key is absent or empties). With a
// count the reply is an array of up to count elements: a null array when the key
// is absent, an empty array when count is zero on an existing key.
func Lpop(cx *shard.Ctx, args [][]byte, r shard.Reply) { popCmd(cx, args, r, true) }
func Rpop(cx *shard.Ctx, args [][]byte, r shard.Reply) { popCmd(cx, args, r, false) }

func popCmd(cx *shard.Ctx, args [][]byte, r shard.Reply, front bool) {
	g := registry(cx)
	key := args[0]
	l, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if len(args) > 2 {
		r.Err(errSyntax)
		return
	}
	// No count: a single bulk or nil.
	if len(args) == 1 {
		if l == nil {
			r.Null()
			return
		}
		v := popOne(l, front)
		if l.length() == 0 {
			g.drop(key)
		}
		r.Bulk(v)
		return
	}
	// Count form.
	count, ok := store.ParseInt(args[1])
	if !ok || count < 0 {
		r.Err(errPosCount)
		return
	}
	if l == nil {
		r.Raw(resp.AppendNullArray(cx.Aux[:0]))
		return
	}
	popped := int(count)
	if popped > l.length() {
		popped = l.length()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], popped)
	for i := 0; i < popped; i++ {
		out = resp.AppendBulk(out, popOne(l, front))
	}
	cx.Aux = out
	r.Raw(out)
	if l.length() == 0 {
		g.drop(key)
	}
}

func popOne(l *list, front bool) []byte {
	if front {
		return l.popFront()
	}
	return l.popBack()
}

// Llen answers LLEN key: the element count, 0 when absent.
func Llen(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if l == nil {
		r.Int(0)
		return
	}
	r.Int(int64(l.length()))
}

// Lindex answers LINDEX key index: the element at a signed index, nil when the
// index is out of range or the key is absent.
func Lindex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	idx, ok := store.ParseInt(args[1])
	if !ok {
		r.Err(errNotInt)
		return
	}
	if l == nil {
		r.Null()
		return
	}
	i := normIndex(int(idx), l.length())
	if i < 0 || i >= l.length() {
		r.Null()
		return
	}
	r.Bulk(l.get(i))
}

// Lset answers LSET key index element: overwrite the element at a signed index.
// A missing key is "no such key"; an out-of-range index is "index out of range".
func Lset(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	idx, ok := store.ParseInt(args[1])
	if !ok {
		r.Err(errNotInt)
		return
	}
	if l == nil {
		r.Err(errNoSuchKey)
		return
	}
	i := normIndex(int(idx), l.length())
	if i < 0 || i >= l.length() {
		r.Err(errOutOfRange)
		return
	}
	l.setAt(i, args[2])
	r.Status("OK")
}

// Lrange answers LRANGE key start stop: the elements in the signed inclusive
// range, an empty array when the range is empty or the key is absent.
func Lrange(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	start, ok1 := store.ParseInt(args[1])
	stop, ok2 := store.ParseInt(args[2])
	if !ok1 || !ok2 {
		r.Err(errNotInt)
		return
	}
	if l == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	lo, hi, ok := clampRange(int(start), int(stop), l.length())
	if !ok {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = resp.AppendBulk(out, l.get(i))
	}
	cx.Aux = out
	r.Raw(out)
}

// Ltrim answers LTRIM key start stop: keep only the elements in the signed
// inclusive range and reply OK. A range that keeps nothing deletes the key.
func Ltrim(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	key := args[0]
	l, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	start, ok1 := store.ParseInt(args[1])
	stop, ok2 := store.ParseInt(args[2])
	if !ok1 || !ok2 {
		r.Err(errNotInt)
		return
	}
	if l == nil {
		r.Status("OK")
		return
	}
	lo, hi, ok := clampRange(int(start), int(stop), l.length())
	if !ok {
		l.trim(1, 0) // empty range, clears the list
	} else {
		l.trim(lo, hi)
	}
	if l.length() == 0 {
		g.drop(key)
	}
	r.Status("OK")
}

// Lrem answers LREM key count element: remove matches under the count-sign rule
// and reply the number removed. The key is deleted when it empties.
func Lrem(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	key := args[0]
	l, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	count, ok := store.ParseInt(args[1])
	if !ok {
		r.Err(errNotInt)
		return
	}
	if l == nil {
		r.Int(0)
		return
	}
	removed := l.remove(int(count), args[2])
	if l.length() == 0 {
		g.drop(key)
	}
	r.Int(int64(removed))
}

// Linsert answers LINSERT key BEFORE|AFTER pivot element: insert before or after
// the first pivot match. Reply the new length, -1 when the pivot is absent, or 0
// when the key is absent.
func Linsert(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var before bool
	switch {
	case eqFold(args[1], "BEFORE"):
		before = true
	case eqFold(args[1], "AFTER"):
		before = false
	default:
		r.Err(errSyntax)
		return
	}
	if l == nil {
		r.Int(0)
		return
	}
	if !l.insert(before, args[2], args[3]) {
		r.Int(-1)
		return
	}
	r.Int(int64(l.length()))
}

// Lpos answers LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen].
// Without COUNT it replies the position of the RANK-th match or nil; with COUNT
// it replies an array of up to count positions (all when count is 0).
func Lpos(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	l, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	target := args[1]
	rank := 1
	count := -1 // -1: COUNT absent (single reply); 0: all; >0: capped
	maxlen := 0
	rest := args[2:]
	for i := 0; i < len(rest); i += 2 {
		if i+1 >= len(rest) {
			r.Err(errSyntax)
			return
		}
		val := rest[i+1]
		switch {
		case eqFold(rest[i], "RANK"):
			v, ok := store.ParseInt(val)
			if !ok {
				r.Err(errNotInt)
				return
			}
			if v == 0 {
				r.Err(errRankZero)
				return
			}
			rank = int(v)
		case eqFold(rest[i], "COUNT"):
			v, ok := store.ParseInt(val)
			if !ok || v < 0 {
				r.Err(errCountNeg)
				return
			}
			count = int(v)
		case eqFold(rest[i], "MAXLEN"):
			v, ok := store.ParseInt(val)
			if !ok || v < 0 {
				r.Err(errMaxlenNeg)
				return
			}
			maxlen = int(v)
		default:
			r.Err(errSyntax)
			return
		}
	}

	// No COUNT: a single position or nil.
	if count < 0 {
		if l == nil {
			r.Null()
			return
		}
		hits := lposScan(l, target, rank, 1, maxlen)
		if len(hits) == 0 {
			r.Null()
			return
		}
		r.Int(int64(hits[0]))
		return
	}
	// COUNT present: an array.
	if l == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	hits := lposScan(l, target, rank, count, maxlen)
	out := resp.AppendArrayHeader(cx.Aux[:0], len(hits))
	for _, h := range hits {
		out = resp.AppendInt(out, int64(h))
	}
	cx.Aux = out
	r.Raw(out)
}

// Object answers OBJECT ENCODING key with the list band first (spec 2064/f3/13
// section 4.4): listpack for the inline band, quicklist for the native band. A
// key that is not a list delegates to the set OBJECT handler, which answers the
// set bands, the string store's encoding, the "no such key" error, and the
// unknown-subcommand error, so this one wiring reports every type. OBJECT is a
// shared verb; routing it through the list here keeps the set slice untouched.
func Object(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if eqFold(args[0], "ENCODING") && len(args) == 2 {
		if l := registry(cx).m[string(args[1])]; l != nil {
			r.Bulk([]byte(l.encoding().String()))
			return
		}
	}
	set.Object(cx, args, r)
}

// --- helpers --------------------------------------------------------------

// normIndex folds a signed index into [0, n); a still-out-of-range result is
// left for the caller to reject.
func normIndex(i, n int) int {
	if i < 0 {
		return i + n
	}
	return i
}

// clampRange folds signed start and stop into a valid inclusive [lo, hi] within
// [0, n). ok is false when the range selects nothing.
func clampRange(start, stop, n int) (lo, hi int, ok bool) {
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
	if start > stop || start >= n || n == 0 {
		return 0, 0, false
	}
	return start, stop, true
}

// lposScan walks the list for target under the RANK, COUNT, and MAXLEN rules and
// returns the matching positions. A positive rank scans head to tail, a negative
// rank tail to head; limit <= 0 collects every match, limit > 0 caps the count;
// maxlen > 0 bounds the number of elements compared.
func lposScan(l *list, target []byte, rank, limit, maxlen int) []int {
	forward := rank > 0
	skip := rank
	if skip < 0 {
		skip = -skip
	}
	skip-- // matches to skip before collecting
	n := l.length()
	var out []int
	compared := 0
	visit := func(i int) bool {
		if maxlen > 0 && compared >= maxlen {
			return false
		}
		compared++
		if !equalBytes(l.get(i), target) {
			return true
		}
		if skip > 0 {
			skip--
			return true
		}
		out = append(out, i)
		return limit <= 0 || len(out) < limit
	}
	if forward {
		for i := 0; i < n; i++ {
			if !visit(i) {
				break
			}
		}
	} else {
		for i := n - 1; i >= 0; i-- {
			if !visit(i) {
				break
			}
		}
	}
	return out
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// eqFold reports whether b equals want under ASCII case folding, for the option
// tokens (BEFORE, AFTER, RANK, COUNT, MAXLEN, ENCODING).
func eqFold(b []byte, want string) bool {
	if len(b) != len(want) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 0x20
		}
		w := want[i]
		if w >= 'a' && w <= 'z' {
			w -= 0x20
		}
		if c != w {
			return false
		}
	}
	return true
}
