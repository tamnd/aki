package command

import (
	"strconv"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// stringCommands returns the string-group command table for this slice. SET and
// GET are the first commands that touch the keyspace. The rest of the string
// family (options on SET, INCR, APPEND, ranges) lands in the strings slice.
func stringCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "set", Group: GroupString, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSet},
		{Name: "get", Group: GroupString, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGet},
	}
}

// handleSet implements SET key value. The option forms (EX, PX, NX, XX,
// KEEPTTL, GET) arrive in the strings slice; for now any extra argument is a
// syntax error so a client never gets a silently wrong result.
func handleSet(ctx *Ctx) {
	if len(ctx.Argv) != 3 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	key, val := ctx.Argv[1], ctx.Argv[2]
	enc := stringEncoding(val)
	if ctx.update(func(db *keyspace.DB) error {
		return db.Set(key, val, keyspace.TypeString, enc, -1)
	}) {
		ctx.Conn.WriteRaw(resp.ReplyOK)
	}
}

// handleGet implements GET key. It returns the value as a bulk string, a null
// bulk string when the key is absent, and WRONGTYPE when the key holds a
// non-string value.
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
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteBulkString(body)
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
