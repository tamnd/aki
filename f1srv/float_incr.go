package f1srv

import (
	"bytes"
	"math"
	"strconv"
)

// The two floating-point increment commands, spec 2064/f1_rewrite_ltm/05 and /11: INCRBYFLOAT
// adds a float to a string key in place, HINCRBYFLOAT does the same to a hash field. Both ride
// the same paths their integer siblings use (the string keyspace for INCRBYFLOAT, the
// element-per-row hash for HINCRBYFLOAT), so this slice is only the float arithmetic and the
// exact Redis reply format layered on top.
//
// A note on precision. Redis does this arithmetic in C long double and formats with
// ld2string(LD_STR_HUMAN), which is "%.17Lf" with trailing zeros trimmed. Go has no long
// double, so we compute in float64 and format the float64 with the same fixed-17 rule. On a
// platform where C long double is itself 64-bit IEEE754 (Apple silicon, and any ARM64 target)
// the two are the same width, so the replies are byte-identical to Redis and Valkey, which is
// the environment the compatibility check runs in. On x86-64, C long double is 80-bit extended,
// so a value that needs more than double precision to round-trip (an increment with more than
// ~16 significant decimal digits, or a long accumulation) can differ in the last digits there.
// Reproducing 80-bit extended arithmetic in Go would need a software long double, a separate
// undertaking; the 64-bit path here is correct for the common domain (counters, prices, small
// decimals) and matches the reference servers used for verification.

// parseRedisFloat parses b the way Redis's string2ld does (via strtold), returning the value
// and whether it is a valid float. It is deliberately not just strconv.ParseFloat: strtold and
// ParseFloat disagree on a few inputs, and each disagreement is reconciled to strtold here so
// the accept/reject boundary matches Redis byte for byte.
//
//   - Hex with no binary exponent ("0x10") is a valid hex float to strtold (16) but a syntax
//     error to ParseFloat, which requires the p-exponent; we retry such input with an explicit
//     "p0" so "0x10" reads as 16, the same as Redis.
//   - An underscore digit separator ("1_000") is accepted by ParseFloat but stops strtold at
//     the underscore, leaving trailing garbage, which Redis rejects; we reject it up front.
//   - NaN parses cleanly in Go but string2ld rejects it, so we reject it explicitly. Infinity
//     parses cleanly in both and is left for the caller's post-add isinf check, matching Redis,
//     where "inf" is a valid float that only fails once it lands in the result.
//   - An overflow literal ("1e400") is ParseFloat's ErrRange (returned as non-nil error, so it
//     falls through to the reject below) and an underflow-to-zero literal ("1e-400", "2e-324")
//     parses to a clean zero in Go but is strtold's ERANGE, which Redis rejects; underflowed
//     catches the second case, which the error path misses.
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

// isHexFloatLiteral reports whether b, after an optional sign, begins with the 0x/0X hex-float
// prefix. It gates the "retry with p0" fixup so only genuine hex input is rewritten.
func isHexFloatLiteral(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	return i+1 < len(b) && b[i] == '0' && (b[i+1] == 'x' || b[i+1] == 'X')
}

// underflowedToZero reports whether b is a numeric literal with a nonzero significand that
// nonetheless parsed to 0.0, which means it underflowed. strtold flags that ERANGE and Redis
// rejects it, while Go returns a clean zero, so this closes that gap. A true zero ("0", "0.0",
// "-0", "0e5") has an all-zero significand and is not rejected. It is only consulted when the
// parse already yielded exactly 0, so the smallest subnormals (which parse to a nonzero value)
// never reach it.
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

// appendHumanFloat renders v the way Redis's ld2string renders a human-friendly long double,
// appending to dst: fixed notation with 17 fractional digits, trailing zeros stripped, a bare
// trailing '.' stripped, and a lone "-0" folded to "0". This is the exact reply INCRBYFLOAT and
// HINCRBYFLOAT echo and the exact bytes a later HGET/GET reads back, so it has to match Redis
// digit for digit. Go's 'f' verb with precision 17 is the counterpart of C's "%.17f": both round
// the same binary value to 17 fractional digits with round-half-to-even.
func appendHumanFloat(dst []byte, v float64) []byte {
	buf := strconv.AppendFloat(nil, v, 'f', 17, 64)
	if bytes.IndexByte(buf, '.') >= 0 {
		i := len(buf)
		for i > 0 && buf[i-1] == '0' {
			i--
		}
		if i > 0 && buf[i-1] == '.' {
			i--
		}
		buf = buf[:i]
	}
	if len(buf) == 2 && buf[0] == '-' && buf[1] == '0' {
		buf = buf[1:]
	}
	return append(dst, buf...)
}

// cmdIncrByFloat implements INCRBYFLOAT: add a float to a string key, treating a missing key as
// zero, and reply with the new value as a bulk string in Redis's human format. The stored value
// must itself be a valid float or it is "ERR value is not a valid float"; a sum that lands on NaN
// or infinity is "ERR increment would produce NaN or Infinity", checked before the write so a
// failed call leaves the value untouched. Like the integer INCR family it keeps any existing TTL,
// which the sibling expire-row model gives for free since the value write never touches that row.
// It takes the key's stripe lock so the read-add-write is atomic against a concurrent writer.
func (c *connState) cmdIncrByFloat(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'incrbyfloat' command")
		return
	}
	incr, ok := parseRedisFloat(argv[2])
	if !ok {
		c.writeErr("ERR value is not a valid float")
		return
	}
	key := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	var oldVal float64
	switch c.resolveType(key) {
	case keyMissing:
	case keyString:
		old, _ := c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = old
		v, ok := parseRedisFloat(old)
		if !ok {
			c.writeErr("ERR value is not a valid float")
			return
		}
		oldVal = v
	default:
		c.writeErr(wrongType)
		return
	}
	sum := oldVal + incr
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		c.writeErr("ERR increment would produce NaN or Infinity")
		return
	}
	var nb [40]byte
	out := appendHumanFloat(nb[:0], sum)
	if err := c.srv.store.Set(key, out); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeBulk(out)
}

// cmdHIncrByFloat implements HINCRBYFLOAT: the hash-field counterpart of INCRBYFLOAT. A missing
// field starts from zero and, once written, brings the field (and, if the hash was empty, the
// hash) into existence and bumps the header count. A field that does not hold a valid float is
// "ERR hash value is not a float", and a NaN or infinite result is "ERR increment would produce
// NaN or Infinity", both checked before the write. It shares HSET's stripe lock so the
// read-add-write and the header count stay consistent.
func (c *connState) cmdHIncrByFloat(argv [][]byte) {
	// HINCRBYFLOAT key field increment
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'hincrbyfloat' command")
		return
	}
	incr, ok := parseRedisFloat(argv[3])
	if !ok {
		c.writeErr("ERR value is not a valid float")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	defer mu.Unlock()
	if c.stringConflict(hkey) {
		c.writeErr(wrongType)
		return
	}
	fk := c.fieldKey(hkey, argv[2])
	// An already-expired field reads as absent: start from zero and recreate it with no TTL. A
	// live-TTL field keeps its TTL through the increment, matching Redis.
	if c.hashHasFieldTTL(hkey) {
		if at, has := c.fieldTTL(fk); has && at <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			fk = c.fieldKey(hkey, argv[2])
		}
	}
	old, exists := c.srv.store.GetKind(fk, c.vbuf[:0], kindHashField)
	c.vbuf = old
	var oldVal float64
	if exists {
		v, ok := parseRedisFloat(old)
		if !ok {
			c.writeErr("ERR hash value is not a float")
			return
		}
		oldVal = v
	}
	sum := oldVal + incr
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		c.writeErr("ERR increment would produce NaN or Infinity")
		return
	}
	// Build the formatted result in a fresh stack buffer: old is backed by vbuf, so keeping the
	// replacement elsewhere stops the store write from reading and writing the same bytes.
	var nb [40]byte
	out := appendHumanFloat(nb[:0], sum)
	isNew, err := c.srv.store.PutKind(fk, out, kindHashField)
	if err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	if isNew {
		c.srv.store.CollInsert(fk, kindHashField)
		if err := c.setHashCount(hkey, c.hashCount(hkey)+1); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	c.writeBulk(out)
}
