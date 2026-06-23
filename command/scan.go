package command

import (
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// scanCommands returns the keyspace iteration commands: KEYS, SCAN and
// RANDOMKEY. They share the live-key walk in the keyspace package and the glob
// matcher below.
func scanCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "keys", Group: GroupGeneric, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly, Handler: handleKeys},
		{Name: "scan", Group: GroupGeneric, Since: "2.8.0",
			Arity: -2, Flags: FlagReadOnly, Handler: handleScan},
		{Name: "randomkey", Group: GroupGeneric, Since: "1.0.0",
			Arity: 1, Flags: FlagReadOnly, Handler: handleRandomKey},
	}
}

// flushForScan drains all pending write-behind writes before a full-keyspace
// read. KEYS, SCAN, and RANDOMKEY must call this so they never miss a key that
// received "+OK" but whose B-tree entry has not yet been applied.
func flushForScan(ctx *Ctx) {
	if ctx.d.engine != nil {
		ctx.d.engine.FlushShardWrites()
	}
}

// handleKeys replies with every key matching the glob pattern. The order is the
// keyspace walk order, which Redis leaves unspecified.
func handleKeys(ctx *Ctx) {
	flushForScan(ctx)
	pattern := ctx.Argv[1]
	var matched [][]byte
	if !ctx.view(func(db *keyspace.DB) error {
		all, err := db.Keys()
		if err != nil {
			return err
		}
		for _, e := range all {
			if stringMatch(pattern, e.Key, false) {
				matched = append(matched, e.Key)
			}
		}
		return nil
	}) {
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(matched))
	for _, k := range matched {
		enc.WriteBulkString(k)
	}
}

// handleRandomKey replies with one key picked uniformly at random, or a null
// when the database is empty.
func handleRandomKey(ctx *Ctx) {
	flushForScan(ctx)
	var chosen []byte
	var found bool
	if !ctx.view(func(db *keyspace.DB) error {
		all, err := db.Keys()
		if err != nil {
			return err
		}
		if len(all) == 0 {
			return nil
		}
		chosen = all[rand.IntN(len(all))].Key
		found = true
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteBulkString(chosen)
}

// handleScan replies with the next cursor and a batch of keys. It accepts the
// MATCH, COUNT and TYPE options in any order.
func handleScan(ctx *Ctx) {
	flushForScan(ctx)
	cursor, err := strconv.ParseUint(string(ctx.Argv[1]), 10, 64)
	if err != nil {
		ctx.enc().WriteError("ERR invalid cursor")
		return
	}
	var match []byte
	var typeFilter string
	count := 10
	for i := 2; i < len(ctx.Argv); {
		opt := strings.ToUpper(string(ctx.Argv[i]))
		if i+1 >= len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		switch opt {
		case "MATCH":
			match = ctx.Argv[i+1]
		case "COUNT":
			n, ok := parseInteger(ctx.Argv[i+1])
			if !ok || n < 1 {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			count = int(n)
		case "TYPE":
			typeFilter = strings.ToLower(string(ctx.Argv[i+1]))
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		i += 2
	}

	var next uint64
	var entries []keyspace.ScanEntry
	if !ctx.view(func(db *keyspace.DB) error {
		n, e, err := db.Scan(cursor, count)
		next, entries = n, e
		return err
	}) {
		return
	}

	var keys [][]byte
	for _, e := range entries {
		if typeFilter != "" && typeName(e.Type) != typeFilter {
			continue
		}
		if match != nil && !stringMatch(match, e.Key, false) {
			continue
		}
		keys = append(keys, e.Key)
	}

	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(strconv.FormatUint(next, 10))
	enc.WriteArrayLen(len(keys))
	for _, k := range keys {
		enc.WriteBulkString(k)
	}
}

// stringMatch reports whether s matches the glob pattern p. It is a byte-for-byte
// port of Redis's stringmatchlen (util.c) so KEYS and SCAN MATCH behave exactly
// like Redis, including the 7.2 negated set [^abc]. nocase folds ASCII case.
func stringMatch(p, s []byte, nocase bool) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if stringMatch(p[1:], s[i:], nocase) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			p, s = p[1:], s[1:]
		case '[':
			p = p[1:]
			negate := false
			if len(p) > 0 && p[0] == '^' {
				negate = true
				p = p[1:]
			}
			matched := false
			for len(p) > 0 {
				if p[0] == ']' {
					p = p[1:]
					break
				}
				if p[0] == '\\' && len(p) > 1 {
					p = p[1:]
					if len(s) > 0 && eqByte(p[0], s[0], nocase) {
						matched = true
					}
					p = p[1:]
				} else if len(p) >= 3 && p[1] == '-' {
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if len(s) > 0 {
						c := s[0]
						if nocase {
							c, lo, hi = toLower(c), toLower(lo), toLower(hi)
						}
						if c >= lo && c <= hi {
							matched = true
						}
					}
					p = p[3:]
				} else {
					if len(s) > 0 && eqByte(p[0], s[0], nocase) {
						matched = true
					}
					p = p[1:]
				}
			}
			if len(s) == 0 {
				return false
			}
			if negate {
				matched = !matched
			}
			if !matched {
				return false
			}
			s = s[1:]
		case '\\':
			if len(p) > 1 {
				p = p[1:]
			}
			fallthrough
		default:
			if len(s) == 0 {
				return false
			}
			if !eqByte(p[0], s[0], nocase) {
				return false
			}
			p, s = p[1:], s[1:]
		}
	}
	return len(s) == 0
}

// eqByte compares two bytes, folding ASCII case when nocase is set.
func eqByte(a, b byte, nocase bool) bool {
	if nocase {
		return toLower(a) == toLower(b)
	}
	return a == b
}

// toLower lowercases an ASCII byte and leaves other bytes alone.
func toLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}
