package str

import (
	"math"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
)

// maxStringLength is the proto-max-bulk-len value size ceiling. SETRANGE
// checks the requested end against it before the write, so a huge offset
// errors with the size text rather than a failed allocation.
const maxStringLength = 512 * 1024 * 1024

// incrErr maps a store arithmetic error to its wire text.
func incrErr(err error) string {
	switch err {
	case store.ErrNotInt:
		return "ERR value is not an integer or out of range"
	case store.ErrOverflow:
		return "ERR increment or decrement would overflow"
	}
	return storeErr(err)
}

func incrBy(cx *shard.Ctx, key []byte, delta int64, r shard.Reply) {
	n, err := cx.St.IncrBy(key, delta, cx.NowMs)
	if err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(incrErr(err))
		return
	}
	if !logCounter(cx, key, n, r) {
		return
	}
	r.Int(n)
}

// Incr answers INCR key.
func Incr(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	incrBy(cx, args[0], 1, r)
}

// Decr answers DECR key.
func Decr(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	incrBy(cx, args[0], -1, r)
}

// IncrByCmd answers INCRBY key delta.
func IncrByCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	incrBy(cx, args[0], n, r)
}

// DecrByCmd answers DECRBY key delta. Negating MinInt64 has no int64 answer,
// so that one argument errors before the store is touched, the Redis text.
func DecrByCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if n == math.MinInt64 {
		r.Err("ERR decrement would overflow")
		return
	}
	incrBy(cx, args[0], -n, r)
}

// IncrByFloat answers INCRBYFLOAT key delta: add a float to a string key,
// treating a missing key as zero, and reply with the new value. The sum is
// checked for NaN and infinity before the write, so a failed call leaves the
// value untouched, and the write keeps any existing deadline. The reply is
// formatted by resp.FormatScore, the shortest round-trip form.
func IncrByFloat(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	incr, ok := store.ParseRedisFloat(args[1])
	if !ok {
		r.Err("ERR value is not a valid float")
		return
	}
	key := args[0]
	var oldVal float64
	old, present := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = old
	if present {
		v, ok := store.ParseRedisFloat(old)
		if !ok {
			r.Err("ERR value is not a valid float")
			return
		}
		oldVal = v
	}
	sum := oldVal + incr
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		r.Err("ERR increment would produce NaN or Infinity")
		return
	}
	var nb [40]byte
	out := resp.FormatScore(nb[:0], sum)
	if err := cx.St.SetString(key, out, cx.NowMs, 0, true); err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(storeErr(err))
		return
	}
	// A float result is not int-shaped, so the frame skips the counter
	// ladder bit; the deadline rode through the keepTTL write above.
	if err := cx.LogStrSet(key, out, cx.St.ExpireAt(key, cx.NowMs), false); err != nil {
		r.Err(err.Error())
		return
	}
	r.Bulk(out)
}

// Append answers APPEND key value: extend the string in place under the
// store's growth policy, creating the key from the argument when absent, and
// reply with the new length. The deadline, if any, rides through.
func Append(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, err := cx.St.Append(args[0], args[1], cx.NowMs)
	if err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(storeErr(err))
		return
	}
	if err := cx.LogStrReadBack(args[0]); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(n)
}

// SetRange answers SETRANGE key offset value: overwrite value at offset,
// zero-filling any gap, and reply with the resulting length. An empty value
// never creates or grows the key; it just reports the current length.
func SetRange(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	offset, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if offset < 0 {
		r.Err("ERR offset is out of range")
		return
	}
	val := args[2]
	if len(val) == 0 {
		n, _ := cx.St.StrLen(args[0], cx.NowMs)
		r.Int(n)
		return
	}
	if offset+int64(len(val)) > maxStringLength {
		r.Err(errStringTooLong)
		return
	}
	n, err := cx.St.SetRange(args[0], int(offset), val, cx.NowMs)
	if err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(storeErr(err))
		return
	}
	if err := cx.LogStrReadBack(args[0]); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(n)
}

// getRangeBytes clamps a GETRANGE (start, end) pair against v, the Redis
// rules: negative indexes count from the end, a wholly negative range whose
// start is right of its end is empty before any folding, and everything else
// clamps into [0, len).
func getRangeBytes(v []byte, start, end int64) []byte {
	strlen := int64(len(v))
	if start < 0 && end < 0 && start > end {
		return nil
	}
	if start < 0 {
		start = strlen + start
	}
	if end < 0 {
		end = strlen + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= strlen {
		end = strlen - 1
	}
	if start > end || strlen == 0 {
		return nil
	}
	return v[start : end+1]
}

// GetRange answers GETRANGE key start end (and its alias SUBSTR): the clamped
// substring, an empty bulk when the range misses or the key is absent. The
// read is a view under the store.GetView lifetime rule; the clamp only
// reslices it and Bulk copies it into the reply arena straight away.
func GetRange(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	start, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	end, ok := store.ParseInt(args[2])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	v, _ := cx.St.GetView(args[0], cx.NowMs)
	r.Bulk(getRangeBytes(v, start, end))
}
