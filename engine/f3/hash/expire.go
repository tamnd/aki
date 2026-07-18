package hash

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The hash field-TTL surface (spec 2064/f3/10 section 6): a per-field expiry that
// Redis 7.4 added and 8.8 carries, the HEXPIRE family. A field's expiry is stored
// inline next to its value, eight absolute-unix-ms bytes on the native record and
// a listpackex slot inline (hash.go, field.go), so a hash that never sets a field
// TTL pays nothing for the machinery. Expiry is lazy: every command reaps fired
// fields on entry (reg.go) before it runs, so a read never returns an expired
// field and a write never overwrites one; the active sweep that reaps untouched
// keys on a timer is deferred to M9. The whole family answers with an array of
// per-field status codes in the FIELDS order the client gave, the shape Redis
// returns.
//
// Each setter is HEXPIRE / HPEXPIRE (relative, seconds and milliseconds) and
// HEXPIREAT / HPEXPIREAT (absolute, seconds and milliseconds), all optionally
// gated by one of NX, XX, GT, or LT. The queries are HTTL / HPTTL (remaining
// seconds and milliseconds), HEXPIRETIME / HPEXPIRETIME (the absolute expiry), and
// HPERSIST (drop the TTL). Every command validates its whole argument tail before
// touching the key, so a syntax error outranks a missing key, matching Redis.

// maxExpireMs is the absolute-ms ceiling Redis 8.8 enforces on a field expiry,
// 2^46-1: a resulting expiry past it is refused with the invalid-expire-time
// error, the same bound the string band's EXPIRE enforces.
const maxExpireMs = int64(1)<<46 - 1

// condFlag is the optional NX/XX/GT/LT gate a setter applies per field.
type condFlag uint8

const (
	condNone condFlag = iota
	condNX            // set only when the field has no TTL
	condXX            // set only when the field already has a TTL
	condGT            // set only when the new expiry is greater than the current
	condLT            // set only when the new expiry is less than the current
)

// Hexpire answers HEXPIRE key seconds [NX|XX|GT|LT] FIELDS n field...: set each
// field's TTL to now plus the given seconds.
func Hexpire(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "hexpire", 1000, false)
}

// Hpexpire is HEXPIRE in milliseconds.
func Hpexpire(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "hpexpire", 1, false)
}

// Hexpireat sets each field's TTL to an absolute unix time in seconds.
func Hexpireat(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "hexpireat", 1000, true)
}

// Hpexpireat sets each field's TTL to an absolute unix time in milliseconds.
func Hpexpireat(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "hpexpireat", 1, true)
}

// expireGeneric is the shared setter body. unitMs is the multiplier that turns
// the argument into milliseconds (1000 for a seconds command, 1 for a ms one);
// absolute distinguishes the AT commands, whose argument is already an absolute
// time, from the relative ones, whose argument is added to now.
func expireGeneric(cx *shard.Ctx, args [][]byte, r shard.Reply, cmd string, unitMs int64, absolute bool) {
	at, aerr := parseExpiry(cmd, args[1], cx.NowMs, unitMs, absolute)
	if aerr != "" {
		r.Err(aerr)
		return
	}
	fields, cond, ferr := parseFieldsClause(cmd, args[2:], true)
	if ferr != "" {
		r.Err(ferr)
		return
	}

	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		// A missing key answers -2 for every requested field, the same as a hash
		// that holds none of them.
		r.Raw(appendCodes(cx.Aux[:0], fields, -2))
		return
	}

	now := uint64(cx.NowMs)
	out := resp.AppendArrayHeader(cx.Aux[:0], len(fields))
	for _, f := range fields {
		code := applyExpiry(h, f, at, cond, now)
		out = resp.AppendInt(out, code)
		// Make the resolved change durable: a set records the field deadline, a set-to-the-
		// past that deleted the field records the field-delete, and a refused or absent field
		// changed nothing so records nothing.
		switch code {
		case 1:
			logFieldExpire(cx, args[0], f, at)
		case 2:
			logDelField(cx, args[0], f)
		}
	}
	cx.Aux = out
	if h.card() == 0 {
		// The last field expired on the spot (a set-to-the-past); Redis drops the
		// hash the moment it empties.
		g.drop(args[0])
	} else {
		// A set-to-the-past deleted a field, or the first field TTL flipped an inline
		// hash to its wider listpackex blob; either way the footprint may have moved,
		// so reconcile the surviving hash.
		g.note(h)
	}
	r.Raw(out)
}

// applyExpiry sets field's expiry to at (absolute ms) under cond and returns the
// Redis status code: -2 the field is absent, 0 the condition refused the change,
// 2 the expiry is at or before now so the field is deleted, 1 the expiry was set.
func applyExpiry(h *hash, f []byte, at int64, cond condFlag, now uint64) int64 {
	if !h.has(f) {
		return -2
	}
	cur := h.fieldExp(f)
	if !condAllows(cond, cur, uint64(at)) {
		return 0
	}
	if at <= int64(now) {
		h.del(f)
		return 2
	}
	h.setFieldExp(f, uint64(at))
	return 1
}

// condAllows reports whether cond permits setting a field currently at cur (0
// meaning no expiry, treated as infinitely far off) to the new expiry at. GT
// never sets a field with no expiry (nothing exceeds infinity) and LT always does
// (everything precedes it), matching Redis.
func condAllows(cond condFlag, cur, at uint64) bool {
	switch cond {
	case condNX:
		return cur == 0
	case condXX:
		return cur != 0
	case condGT:
		return cur != 0 && at > cur
	case condLT:
		return cur == 0 || at < cur
	default:
		return true
	}
}

// ttlMode is which TTL query a reader answers.
type ttlMode uint8

const (
	ttlSeconds  ttlMode = iota // HTTL: remaining seconds
	ttlMillis                  // HPTTL: remaining milliseconds
	ttlAtSecs                  // HEXPIRETIME: absolute unix seconds
	ttlAtMillis                // HPEXPIRETIME: absolute unix milliseconds
)

// Httl answers HTTL key FIELDS n field...: the remaining seconds per field.
func Httl(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, "httl", ttlSeconds)
}

// Hpttl is HTTL in milliseconds.
func Hpttl(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, "hpttl", ttlMillis)
}

// Hexpiretime answers the absolute expiry unix time in seconds per field.
func Hexpiretime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, "hexpiretime", ttlAtSecs)
}

// Hpexpiretime is HEXPIRETIME in milliseconds.
func Hpexpiretime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, "hpexpiretime", ttlAtMillis)
}

// ttlGeneric is the shared query body: -2 the field is absent, -1 it is present
// with no TTL, else the remaining or absolute time under the mode's unit.
func ttlGeneric(cx *shard.Ctx, args [][]byte, r shard.Reply, cmd string, mode ttlMode) {
	fields, _, ferr := parseFieldsClause(cmd, args[1:], false)
	if ferr != "" {
		r.Err(ferr)
		return
	}
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Raw(appendCodes(cx.Aux[:0], fields, -2))
		return
	}
	now := cx.NowMs
	out := resp.AppendArrayHeader(cx.Aux[:0], len(fields))
	for _, f := range fields {
		out = resp.AppendInt(out, ttlValue(h, f, mode, now))
	}
	cx.Aux = out
	r.Raw(out)
}

// ttlValue resolves one field's TTL query.
func ttlValue(h *hash, f []byte, mode ttlMode, now int64) int64 {
	if !h.has(f) {
		return -2
	}
	exp := h.fieldExp(f)
	if exp == 0 {
		return -1
	}
	switch mode {
	case ttlMillis:
		return int64(exp) - now
	case ttlAtSecs:
		return (int64(exp) + 500) / 1000
	case ttlAtMillis:
		return int64(exp)
	default: // ttlSeconds
		return (int64(exp) - now + 500) / 1000
	}
}

// Hpersist answers HPERSIST key FIELDS n field...: drop each field's TTL, 1 when
// it had one, -1 when present without a TTL, -2 when absent. The sticky
// listpackex encoding stays; only the expiry clears (spec 2064/f3/10 section 6.4).
func Hpersist(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	fields, _, ferr := parseFieldsClause("hpersist", args[1:], false)
	if ferr != "" {
		r.Err(ferr)
		return
	}
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Raw(appendCodes(cx.Aux[:0], fields, -2))
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], len(fields))
	for _, f := range fields {
		code := persistField(h, f)
		out = resp.AppendInt(out, code)
		// A field whose TTL was actually cleared records a zero-deadline field-expire, so a
		// replay drops the deadline instead of restoring it from an earlier effect.
		if code == 1 {
			logFieldExpire(cx, args[0], f, 0)
		}
	}
	cx.Aux = out
	r.Raw(out)
}

// persistField clears one field's TTL and returns its status code.
func persistField(h *hash, f []byte) int64 {
	if !h.has(f) {
		return -2
	}
	if h.fieldExp(f) == 0 {
		return -1
	}
	h.clearFieldExp(f)
	return 1
}

// getexMode is what HGETEX does to each field's TTL alongside the read.
type getexMode uint8

const (
	getexNone    getexMode = iota // no option: read only, leave every TTL as is
	getexPersist                  // PERSIST: clear each read field's TTL
	getexSet                      // EX/PX/EXAT/PXAT: set each read field's TTL to at
)

// Hgetex answers HGETEX key [EX s | PX ms | EXAT ts | PXAT tms | PERSIST] FIELDS n
// field...: return each field's value, nil when it is absent, and reset or clear
// its TTL in the same step. Values are read into the reply before any TTL change,
// so a set-to-the-past that deletes a field still answers the value it held. No
// option leaves every field's TTL untouched, the plain HMGET read (spec 2064/f3/10
// section 7.4).
func Hgetex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	at, mode, rest, oerr := parseGetexOption("hgetex", args[1:], cx.NowMs)
	if oerr != "" {
		r.Err(oerr)
		return
	}
	fields, _, ferr := parseFieldsClause("hgetex", rest, false)
	if ferr != "" {
		r.Err(ferr)
		return
	}
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], len(fields))
	if h == nil {
		for range fields {
			out = resp.AppendNull(out)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}
	now := uint64(cx.NowMs)
	for _, f := range fields {
		if v, ok := h.get(f); ok {
			out = resp.AppendBulk(out, v)
		} else {
			out = resp.AppendNull(out)
		}
	}
	cx.Aux = out
	switch mode {
	case getexSet:
		for _, f := range fields {
			switch applyExpiry(h, f, at, condNone, now) {
			case 1:
				logFieldExpire(cx, args[0], f, at)
			case 2:
				logDelField(cx, args[0], f)
			}
		}
	case getexPersist:
		for _, f := range fields {
			if persistField(h, f) == 1 {
				logFieldExpire(cx, args[0], f, 0)
			}
		}
	}
	// A read-only HGETEX changes no footprint, but a TTL set may have flipped the
	// hash to its listpackex blob or deleted a set-to-the-past field, so reconcile
	// whenever an option ran.
	if mode != getexNone {
		if h.card() == 0 {
			g.drop(args[0])
		} else {
			g.note(h)
		}
	}
	r.Raw(out)
}

// parseGetexOption reads HGETEX's optional leading TTL directive and returns the
// resulting absolute-ms expiry (for getexSet), the mode, the remaining tail that
// begins the FIELDS clause, or a Redis error string. An absent or unrecognized
// leading token is no option: the tail is returned whole for the FIELDS parser to
// judge, so a bogus token surfaces as its unknown-argument error, not here.
func parseGetexOption(cmd string, rest [][]byte, now int64) (int64, getexMode, [][]byte, string) {
	if len(rest) == 0 {
		return 0, getexNone, rest, ""
	}
	if eqFold(rest[0], "PERSIST") {
		return 0, getexPersist, rest[1:], ""
	}
	var unitMs int64
	var absolute bool
	switch {
	case eqFold(rest[0], "EX"):
		unitMs, absolute = 1000, false
	case eqFold(rest[0], "PX"):
		unitMs, absolute = 1, false
	case eqFold(rest[0], "EXAT"):
		unitMs, absolute = 1000, true
	case eqFold(rest[0], "PXAT"):
		unitMs, absolute = 1, true
	default:
		return 0, getexNone, rest, ""
	}
	if len(rest) < 2 {
		return 0, getexNone, nil, "ERR wrong number of arguments for '" + cmd + "' command"
	}
	at, aerr := parseExpiry(cmd, rest[1], now, unitMs, absolute)
	if aerr != "" {
		return 0, getexNone, nil, aerr
	}
	return at, getexSet, rest[2:], ""
}

// parseExpiry reads a setter's time argument and returns the resulting absolute
// unix-ms expiry, or a Redis error string. A negative argument is refused up front
// (the must-be-non-negative error, all four setters), and a result past the 2^46-1
// ceiling is refused with the invalid-expire-time error naming the command. The
// argument magnitude is bounded before the multiply so it cannot overflow int64.
func parseExpiry(cmd string, raw []byte, now, unitMs int64, absolute bool) (int64, string) {
	v, ok := store.ParseInt(raw)
	if !ok {
		return 0, "ERR value is not an integer or out of range"
	}
	if v < 0 {
		return 0, "ERR invalid expire time, must be >= 0"
	}
	if v > maxExpireMs/unitMs {
		// Even read as an absolute time this already tops the ceiling, and the guard
		// keeps v*unitMs from overflowing.
		return 0, "ERR invalid expire time in '" + cmd + "' command"
	}
	at := v * unitMs
	if !absolute {
		at += now
	}
	if at > maxExpireMs {
		return 0, "ERR invalid expire time in '" + cmd + "' command"
	}
	return at, ""
}

// parseFieldsClause parses the optional condition and the mandatory FIELDS clause
// shared by every command in the family, over rest (the arguments after the key
// and, for a setter, the time). It returns the field slice, the condition, or a
// Redis error string. The FIELDS keyword is located first: a token before it must
// be a condition when allowCond is set, else it is an unknown argument, and its
// absence is a wrong-number-of-arguments error. numFields must be a positive
// integer and must count the fields that follow exactly; a short count is a
// wrong-number-of-arguments error and a long one names the first extra token, all
// matching Redis 8.8 wording.
func parseFieldsClause(cmd string, rest [][]byte, allowCond bool) ([][]byte, condFlag, string) {
	fi := -1
	for i, t := range rest {
		if eqFold(t, "FIELDS") {
			fi = i
			break
		}
	}
	if fi < 0 {
		return nil, condNone, "ERR wrong number of arguments for '" + cmd + "' command"
	}
	cond := condNone
	for i := 0; i < fi; i++ {
		c := parseCond(rest[i])
		if c == condNone || !allowCond {
			return nil, condNone, "ERR unknown argument: " + string(rest[i])
		}
		if cond != condNone {
			return nil, condNone, "ERR Multiple condition flags specified"
		}
		cond = c
	}
	after := rest[fi+1:]
	if len(after) == 0 {
		return nil, condNone, "ERR wrong number of arguments for '" + cmd + "' command"
	}
	n, ok := store.ParseInt(after[0])
	if !ok || n <= 0 {
		return nil, condNone, "ERR Parameter `numFields` should be greater than 0"
	}
	fields := after[1:]
	if int64(len(fields)) < n {
		return nil, condNone, "ERR wrong number of arguments"
	}
	if int64(len(fields)) > n {
		return nil, condNone, "ERR unknown argument: " + string(fields[n])
	}
	return fields, cond, ""
}

// parseCond maps a token to its condition flag, condNone when it is not one.
func parseCond(t []byte) condFlag {
	switch {
	case eqFold(t, "NX"):
		return condNX
	case eqFold(t, "XX"):
		return condXX
	case eqFold(t, "GT"):
		return condGT
	case eqFold(t, "LT"):
		return condLT
	default:
		return condNone
	}
}

// appendCodes frames an array of the same status code, one per field, the
// missing-key reply (all -2).
func appendCodes(dst []byte, fields [][]byte, code int64) []byte {
	dst = resp.AppendArrayHeader(dst, len(fields))
	for range fields {
		dst = resp.AppendInt(dst, code)
	}
	return dst
}
