package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// aggScanCommands returns the per-key scan commands HSCAN, SSCAN and ZSCAN.
func aggScanCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hscan", Group: GroupHash, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHScan},
		{Name: "sscan", Group: GroupSet, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSScan},
		{Name: "zscan", Group: GroupSortedSet, Since: "2.8.0",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleZScan},
	}
}

// aggScanOpts holds the parsed MATCH, COUNT and NOVALUES options shared by the
// per-key scans.
type aggScanOpts struct {
	match    []byte
	novalues bool
}

// parseAggScan reads the cursor and the MATCH, COUNT and optional NOVALUES tail.
// The cursor is accepted but not used: every aggregate fits in one call at this
// milestone, so the scan returns all matching elements and a zero cursor. COUNT
// is validated as a hint and otherwise ignored.
func parseAggScan(argv [][]byte, allowNoValues bool) (aggScanOpts, string, bool) {
	var o aggScanOpts
	if _, err := strconv.ParseUint(string(argv[2]), 10, 64); err != nil {
		return o, "ERR invalid cursor", false
	}
	for i := 3; i < len(argv); {
		opt := strings.ToUpper(string(argv[i]))
		switch opt {
		case "MATCH":
			if i+1 >= len(argv) {
				return o, "ERR syntax error", false
			}
			o.match = argv[i+1]
			i += 2
		case "COUNT":
			if i+1 >= len(argv) {
				return o, "ERR syntax error", false
			}
			n, ok := parseInteger(argv[i+1])
			if !ok || n < 1 {
				return o, "ERR syntax error", false
			}
			i += 2
		case "NOVALUES":
			if !allowNoValues {
				return o, "ERR syntax error", false
			}
			o.novalues = true
			i++
		default:
			return o, "ERR syntax error", false
		}
	}
	return o, "", true
}

// matchMember reports whether a member passes the optional MATCH filter. An empty
// pattern matches everything.
func matchMember(pattern, member []byte) bool {
	return pattern == nil || stringMatch(pattern, member, false)
}

// handleHScan returns a hash's fields, with their values unless NOVALUES is set.
func handleHScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, true)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	var fields []hashField
	var wrongTyp bool
	if !ctx.view(func(db *keyspace.DB) error {
		fs, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		fields = fs
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr("0")
	var kept []hashField
	for _, f := range fields {
		if matchMember(o.match, f.field) {
			kept = append(kept, f)
		}
	}
	if o.novalues {
		enc.WriteArrayLen(len(kept))
		for _, f := range kept {
			enc.WriteBulkString(f.field)
		}
		return
	}
	enc.WriteArrayLen(len(kept) * 2)
	for _, f := range kept {
		enc.WriteBulkString(f.field)
		enc.WriteBulkString(f.value)
	}
}

// handleSScan returns a set's members.
func handleSScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, false)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	var members [][]byte
	var wrongTyp bool
	if !ctx.view(func(db *keyspace.DB) error {
		ms, hdr, found, err := getSet(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		members = ms
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	var kept [][]byte
	for _, m := range members {
		if matchMember(o.match, m) {
			kept = append(kept, m)
		}
	}
	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr("0")
	enc.WriteArrayLen(len(kept))
	for _, m := range kept {
		enc.WriteBulkString(m)
	}
}

// handleZScan returns a sorted set's members with their scores as bulk strings.
func handleZScan(ctx *Ctx) {
	key := ctx.Argv[1]
	o, errStr, ok := parseAggScan(ctx.Argv, false)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	var members []zmember
	var wrongTyp bool
	if !ctx.view(func(db *keyspace.DB) error {
		ms, hdr, found, err := getZSet(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		members = ms
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	var kept []zmember
	for _, m := range members {
		if matchMember(o.match, m.member) {
			kept = append(kept, m)
		}
	}
	enc := ctx.enc()
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr("0")
	enc.WriteArrayLen(len(kept) * 2)
	for _, m := range kept {
		enc.WriteBulkString(m.member)
		enc.WriteBulkStringStr(resp.FormatDouble(m.score))
	}
}
