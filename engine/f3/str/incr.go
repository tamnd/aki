package str

import (
	"bytes"
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
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
		r.Err(incrErr(err))
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

// parseRedisFloat parses b the way Redis's string2ld does (via strtold),
// returning the value and whether it is valid. strconv.ParseFloat disagrees
// with strtold on a few inputs, and each disagreement is reconciled here so
// the accept/reject boundary matches byte for byte: hex with no binary
// exponent retries with an explicit p0, an underscore separator rejects, NaN
// rejects, and a literal that underflowed to zero rejects. Infinity parses
// cleanly and only fails once it lands in a result.
func parseRedisFloat(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	if bytes.IndexByte(b, '_') >= 0 {
		return 0, false
	}
	s := string(b)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrSyntax && isHexFloatLiteral(b) {
			v, err = strconv.ParseFloat(s+"p0", 64)
		}
	}
	if err != nil {
		return 0, false
	}
	if math.IsNaN(v) {
		return 0, false
	}
	if v == 0 && underflowedToZero(b) {
		return 0, false
	}
	return v, true
}

// isHexFloatLiteral reports whether b, after an optional sign, begins with
// the 0x/0X hex-float prefix. It gates the p0 retry so only genuine hex input
// is rewritten.
func isHexFloatLiteral(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	return i+1 < len(b) && b[i] == '0' && (b[i+1] == 'x' || b[i+1] == 'X')
}

// underflowedToZero reports whether b has a nonzero significand yet parsed to
// exactly 0.0, which means it underflowed: strtold flags that ERANGE and
// Redis rejects it, while Go returns a clean zero. Only consulted when the
// parse already yielded zero.
func underflowedToZero(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	hex := i+1 < len(b) && b[i] == '0' && (b[i+1] == 'x' || b[i+1] == 'X')
	if hex {
		i += 2
	}
	for ; i < len(b); i++ {
		d := b[i]
		if hex {
			if d == 'p' || d == 'P' {
				break
			}
			if (d >= '1' && d <= '9') || (d >= 'a' && d <= 'f') || (d >= 'A' && d <= 'F') {
				return true
			}
		} else {
			if d == 'e' || d == 'E' {
				break
			}
			if d >= '1' && d <= '9' {
				return true
			}
		}
	}
	return false
}

// IncrByFloat answers INCRBYFLOAT key delta: add a float to a string key,
// treating a missing key as zero, and reply with the new value. The sum is
// checked for NaN and infinity before the write, so a failed call leaves the
// value untouched, and the write keeps any existing deadline. The reply is
// formatted by resp.FormatScore, the shortest round-trip form.
func IncrByFloat(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	incr, ok := parseRedisFloat(args[1])
	if !ok {
		r.Err("ERR value is not a valid float")
		return
	}
	key := args[0]
	var oldVal float64
	old, present := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = old
	if present {
		v, ok := parseRedisFloat(old)
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
		r.Err(storeErr(err))
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
		r.Err(storeErr(err))
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
		r.Err(storeErr(err))
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
// substring, an empty bulk when the range misses or the key is absent.
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
	v, _ := cx.St.GetString(args[0], cx.NowMs, cx.Val)
	cx.Val = v
	r.Bulk(getRangeBytes(v, start, end))
}
