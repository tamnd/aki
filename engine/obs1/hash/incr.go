package hash

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
)

// The hash arithmetic verbs (spec 2064/f3/10 section 7.3). Both read the field's
// current value as a number, treating an absent field or key as zero, apply the
// increment, write the canonical rendering back into the same field, and reply
// the new value. They share the string band's numeric parsers (store.ParseInt
// for HINCRBY, store.ParseRedisFloat for HINCRBYFLOAT) so the two bands accept
// and reject exactly the same literals. Both parse the increment before touching
// the registry and, when the increment is well formed but the result is refused
// (integer overflow, or a float sum that lands on NaN or Infinity), leave the
// keyspace untouched rather than strand an empty hash, the same discipline
// str.IncrByFloat keeps.

// Hincrby answers HINCRBY key field delta: add a signed integer to the field's
// integer value, creating the field at zero when absent and the hash when the
// key is absent, and reply the new value. A non-integer field value or a delta
// that would overflow int64 errors without changing anything.
func Hincrby(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	delta, ok := store.ParseInt(args[2])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var cur, keepAt int64
	if h != nil {
		if v, ok := h.get(args[1]); ok {
			cur, ok = store.ParseInt(v)
			if !ok {
				r.Err("ERR hash value is not an integer")
				return
			}
			// HINCRBY preserves the field's TTL where HSET clears it, so the
			// emission restores the deadline behind the hset frame it writes.
			keepAt = int64(h.fieldExp(args[1]))
		}
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		r.Err("ERR increment or decrement would overflow")
		return
	}
	sum := cur + delta
	created := false
	if h == nil {
		_, h, created, _ = getOrCreate(cx, args[0])
	}
	var nb [20]byte
	out := strconv.AppendInt(nb[:0], sum, 10)
	h.set(args[1], out)
	g.note(h)
	if err := cx.LogHashSet(args[0], created, [][]byte{args[1], out}, keepAt); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(sum)
}

// Hincrbyfloat answers HINCRBYFLOAT key field delta: add a float to the field's
// numeric value, creating the field at zero when absent and the hash when the
// key is absent, and reply the new value formatted the shortest round-trip way
// (resp.FormatScore, Redis's ld2string). A non-float field value errors, and a
// sum that is NaN or Infinity errors, both without changing anything.
func Hincrbyfloat(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	incr, ok := store.ParseRedisFloat(args[2])
	if !ok {
		r.Err("ERR value is not a valid float")
		return
	}
	// Redis 8.8 rejects an infinite increment argument up front with its own
	// message, distinct from the "increment would produce" message a finite
	// increment gets when the sum overflows to infinity below. NaN is already
	// refused by ParseRedisFloat as not-a-float, matching Redis. This runs before
	// the registry lookup so an infinite increment on a missing key strands no
	// hash, the behavior a live redis-server shows.
	if math.IsInf(incr, 0) {
		r.Err("ERR value is NaN or Infinity")
		return
	}
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var cur float64
	var keepAt int64
	if h != nil {
		if v, ok := h.get(args[1]); ok {
			cur, ok = store.ParseRedisFloat(v)
			if !ok {
				r.Err("ERR hash value is not a float")
				return
			}
			// HINCRBYFLOAT preserves the field's TTL, the same restore HINCRBY emits.
			keepAt = int64(h.fieldExp(args[1]))
		}
	}
	sum := cur + incr
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		r.Err("ERR increment would produce NaN or Infinity")
		return
	}
	var nb [40]byte
	out := resp.FormatScore(nb[:0], sum)
	created := false
	if h == nil {
		_, h, created, _ = getOrCreate(cx, args[0])
	}
	h.set(args[1], out)
	g.note(h)
	if err := cx.LogHashSet(args[0], created, [][]byte{args[1], out}, keepAt); err != nil {
		r.Err(err.Error())
		return
	}
	r.Bulk(out)
}
