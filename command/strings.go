package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// stringCommands returns the string-group command table covering SET with its
// full option grammar, GET, and the SET/GET aliases that bundle a TTL or a
// conditional write. The multi-key commands, counters and ranges land in later
// string slices.
func stringCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "set", Group: GroupString, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSet},
		{Name: "setnx", Group: GroupString, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSetNX},
		{Name: "setex", Group: GroupString, Since: "2.0.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSetEX},
		{Name: "psetex", Group: GroupString, Since: "2.6.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handlePSetEX},
		{Name: "get", Group: GroupString, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGet},
		{Name: "getset", Group: GroupString, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetSet},
		{Name: "getdel", Group: GroupString, Since: "6.2.0",
			Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetDel},
		{Name: "getex", Group: GroupString, Since: "6.2.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetEX},
		{Name: "mset", Group: GroupString, Since: "1.0.1",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 2,
			Handler: handleMSet},
		{Name: "msetnx", Group: GroupString, Since: "1.0.1",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 2,
			Handler: handleMSetNX},
		{Name: "mget", Group: GroupString, Since: "1.0.0",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handleMGet},
		{Name: "append", Group: GroupString, Since: "2.0.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleAppend},
		{Name: "strlen", Group: GroupString, Since: "2.2.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleStrlen},
		{Name: "setrange", Group: GroupString, Since: "2.2.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSetRange},
		{Name: "getrange", Group: GroupString, Since: "2.4.0",
			Arity: 4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetRange},
		{Name: "substr", Group: GroupString, Since: "1.0.0",
			Arity: 4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetRange},
		{Name: "incr", Group: GroupString, Since: "1.0.0",
			Arity: 2, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleIncr},
		{Name: "decr", Group: GroupString, Since: "1.0.0",
			Arity: 2, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleDecr},
		{Name: "incrby", Group: GroupString, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleIncrBy},
		{Name: "decrby", Group: GroupString, Since: "1.0.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleDecrBy},
		{Name: "incrbyfloat", Group: GroupString, Since: "2.6.0",
			Arity: 3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleIncrByFloat},
		{Name: "lcs", Group: GroupString, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleLCS},
	}
}

// TTL modes for the SET option grammar.
const (
	ttlNone uint8 = iota
	ttlEX
	ttlPX
	ttlEXAT
	ttlPXAT
	ttlKeep
	ttlPersist
)

// setOptions is the parsed form of the SET option list (doc 08 §3.2, §4).
type setOptions struct {
	nx       bool
	xx       bool
	get      bool
	ttlMode  uint8
	ttlValue int64
}

// parseSetOptions parses the option words after "SET key value" (doc 08 §4). It
// returns ok=false with a ready-to-send error reply for a malformed list: a
// syntax error for an unknown word or a conflicting pair, or the invalid-expire
// error for a non-positive TTL. IDLE and FREQ from Redis 7.4 are accepted and
// validated but, with no eviction metadata maintained yet, their values are not
// stored.
func parseSetOptions(args [][]byte) (setOptions, string, bool) {
	opts := setOptions{}
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			if opts.xx {
				return opts, "ERR syntax error", false
			}
			opts.nx = true
		case "XX":
			if opts.nx {
				return opts, "ERR syntax error", false
			}
			opts.xx = true
		case "GET":
			opts.get = true
		case "EX", "PX", "EXAT", "PXAT":
			mode := ttlModeFor(string(args[i]))
			if opts.ttlMode != ttlNone {
				return opts, "ERR syntax error", false
			}
			i++
			if i >= len(args) {
				return opts, "ERR syntax error", false
			}
			v, ok := parseInteger(args[i])
			if !ok || v <= 0 {
				return opts, "ERR invalid expire time in 'set' command", false
			}
			opts.ttlMode = mode
			opts.ttlValue = v
		case "KEEPTTL":
			if opts.ttlMode != ttlNone {
				return opts, "ERR syntax error", false
			}
			opts.ttlMode = ttlKeep
		case "IDLE", "FREQ":
			i++
			if i >= len(args) {
				return opts, "ERR syntax error", false
			}
			v, ok := parseInteger(args[i])
			if !ok || v < 0 {
				return opts, "ERR syntax error", false
			}
		default:
			return opts, "ERR syntax error", false
		}
	}
	return opts, "", true
}

// ttlModeFor maps an option word to its TTL mode.
func ttlModeFor(word string) uint8 {
	switch strings.ToUpper(word) {
	case "EX":
		return ttlEX
	case "PX":
		return ttlPX
	case "EXAT":
		return ttlEXAT
	case "PXAT":
		return ttlPXAT
	default:
		return ttlNone
	}
}

// absoluteTTL turns a parsed TTL mode and value into the absolute millisecond
// deadline Set stores, given the previous header for KEEPTTL. It returns -1 for
// no expiry.
func absoluteTTL(mode uint8, value int64, prev keyspace.ValueHeader, found bool) int64 {
	switch mode {
	case ttlEX:
		return keyspace.NowMillis() + value*1000
	case ttlPX:
		return keyspace.NowMillis() + value
	case ttlEXAT:
		return value * 1000
	case ttlPXAT:
		return value
	case ttlKeep:
		if found && prev.HasTTL() {
			return prev.TTLms
		}
		return -1
	default:
		return -1
	}
}

// handleSet implements SET key value with the full NX/XX/GET and EX/PX/EXAT/
// PXAT/KEEPTTL option grammar (doc 08 §3.2). The whole operation runs inside one
// update closure so the old-value read, the condition check and the write are
// atomic against other writers.
func handleSet(ctx *Ctx) {
	key, val := ctx.Argv[1], ctx.Argv[2]
	opts, errMsg, ok := parseSetOptions(ctx.Argv[3:])
	if !ok {
		ctx.enc().WriteError(errMsg)
		return
	}

	var (
		wrongTyp bool
		aborted  bool // a condition (NX or XX) was not met
		oldBody  []byte
		oldFound bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		prevBody, prevHdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if opts.get && found && prevHdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		oldFound = found
		if found {
			oldBody = prevBody
		}
		if (opts.nx && found) || (opts.xx && !found) {
			aborted = true
			return nil
		}
		ttl := absoluteTTL(opts.ttlMode, opts.ttlValue, prevHdr, found)
		return db.Set(key, val, keyspace.TypeString, stringEncoding(val), ttl)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	// With GET the reply is always the old value (or null); without GET it is
	// OK on a real write and null when a condition blocked the write.
	if opts.get {
		writeStringOrNull(ctx, oldBody, oldFound)
		return
	}
	if aborted {
		ctx.enc().WriteNull()
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleSetNX implements SETNX key value, the legacy form of SET ... NX. It
// returns 1 when the key was set and 0 when it already existed.
func handleSetNX(ctx *Ctx) {
	key, val := ctx.Argv[1], ctx.Argv[2]
	var stored bool
	if ctx.update(func(db *keyspace.DB) error {
		exists, err := db.Exists(key)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		stored = true
		return db.Set(key, val, keyspace.TypeString, stringEncoding(val), -1)
	}) {
		ctx.enc().WriteInteger(boolToInt(stored))
	}
}

// handleSetEX implements SETEX key seconds value, the legacy form of
// SET ... EX seconds.
func handleSetEX(ctx *Ctx) {
	setWithExpire(ctx, ttlEX, "setex")
}

// handlePSetEX implements PSETEX key milliseconds value, the legacy form of
// SET ... PX milliseconds.
func handlePSetEX(ctx *Ctx) {
	setWithExpire(ctx, ttlPX, "psetex")
}

// setWithExpire backs SETEX and PSETEX. The TTL argument sits before the value:
// SETEX key seconds value. The expire must be a positive integer.
func setWithExpire(ctx *Ctx, mode uint8, name string) {
	key, val := ctx.Argv[1], ctx.Argv[3]
	v, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	if v <= 0 {
		ctx.enc().WriteError("ERR invalid expire time in '" + name + "' command")
		return
	}
	if ctx.update(func(db *keyspace.DB) error {
		ttl := absoluteTTL(mode, v, keyspace.ValueHeader{}, false)
		return db.Set(key, val, keyspace.TypeString, stringEncoding(val), ttl)
	}) {
		ctx.Conn.WriteRaw(resp.ReplyOK)
	}
}

// handleGet implements GET key. It returns the value as a bulk string, null when
// the key is absent, and WRONGTYPE when the key holds a non-string value.
func handleGet(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		body     []byte
		found    bool
		wrongTyp bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		b, hdr, f, err := db.Get(key)
		if err != nil {
			return err
		}
		if f && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		body, found = b, f
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	writeStringOrNull(ctx, body, found)
}

// handleGetSet implements GETSET key value: return the old value, then set the
// new one. It clears any existing TTL, the same as SET without KEEPTTL.
func handleGetSet(ctx *Ctx) {
	key, val := ctx.Argv[1], ctx.Argv[2]
	var (
		wrongTyp bool
		oldBody  []byte
		oldFound bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		prevBody, prevHdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && prevHdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		oldBody, oldFound = prevBody, found
		return db.Set(key, val, keyspace.TypeString, stringEncoding(val), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	writeStringOrNull(ctx, oldBody, oldFound)
}

// handleGetDel implements GETDEL key: return the value, then delete the key. A
// non-string value is a WRONGTYPE error and the key is left in place.
func handleGetDel(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		wrongTyp bool
		body     []byte
		found    bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		b, hdr, f, err := db.Get(key)
		if err != nil {
			return err
		}
		if f && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		body, found = b, f
		if found {
			_, err = db.Delete(key)
		}
		return err
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	writeStringOrNull(ctx, body, found)
}

// handleGetEX implements GETEX key [EX|PX|EXAT|PXAT value | PERSIST]: return the
// value and optionally change the TTL in one atomic step. With no option it is a
// plain read.
func handleGetEX(ctx *Ctx) {
	key := ctx.Argv[1]
	mode, value, errMsg, ok := parseGetExOptions(ctx.Argv[2:])
	if !ok {
		ctx.enc().WriteError(errMsg)
		return
	}
	var (
		wrongTyp bool
		body     []byte
		found    bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		b, hdr, f, err := db.Get(key)
		if err != nil {
			return err
		}
		if f && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		body, found = b, f
		if !found || mode == ttlNone {
			return nil
		}
		if mode == ttlPersist && !hdr.HasTTL() {
			return nil
		}
		ttl := absoluteTTL(mode, value, hdr, found)
		return db.Set(key, b, keyspace.TypeString, hdr.Encoding, ttl)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	writeStringOrNull(ctx, body, found)
}

// parseGetExOptions parses the single optional TTL clause of GETEX (doc 08 §3.9).
// At most one of EX/PX/EXAT/PXAT/PERSIST is allowed.
func parseGetExOptions(args [][]byte) (mode uint8, value int64, errMsg string, ok bool) {
	if len(args) == 0 {
		return ttlNone, 0, "", true
	}
	switch strings.ToUpper(string(args[0])) {
	case "PERSIST":
		if len(args) != 1 {
			return ttlNone, 0, "ERR syntax error", false
		}
		return ttlPersist, 0, "", true
	case "EX", "PX", "EXAT", "PXAT":
		if len(args) != 2 {
			return ttlNone, 0, "ERR syntax error", false
		}
		v, parsed := parseInteger(args[1])
		if !parsed || v <= 0 {
			return ttlNone, 0, "ERR invalid expire time in 'getex' command", false
		}
		return ttlModeFor(string(args[0])), v, "", true
	default:
		return ttlNone, 0, "ERR syntax error", false
	}
}

// writeStringOrNull writes body as a bulk string when found, or a null reply
// otherwise.
func writeStringOrNull(ctx *Ctx, body []byte, found bool) {
	if found {
		ctx.enc().WriteBulkString(body)
		return
	}
	ctx.enc().WriteNull()
}

// boolToInt maps a Go bool to the 0/1 integer Redis returns from a conditional
// write.
func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// stringEncoding picks the OBJECT ENCODING a freshly stored string would report
// (doc 05 §3.3): int for a canonical 64-bit integer, embstr for a short string,
// raw otherwise. The thresholds match Redis 7.x.
func stringEncoding(val []byte) uint8 {
	if isCanonicalInt(val) {
		return keyspace.EncInt
	}
	if len(val) <= 44 {
		return keyspace.EncEmbStr
	}
	return keyspace.EncRaw
}

// isCanonicalInt reports whether val is the canonical base-10 form of a signed
// 64-bit integer, the same test Redis uses to choose the int encoding. Leading
// zeros, a plus sign, and surrounding space all fail the round-trip check.
func isCanonicalInt(val []byte) bool {
	if len(val) == 0 || len(val) > 20 {
		return false
	}
	n, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return false
	}
	return strconv.FormatInt(n, 10) == string(val)
}
