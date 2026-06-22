package command

import (
	"slices"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// hashTTLCommands returns the per-field expiry commands for hashes (Redis 7.4).
// Each field can carry its own absolute deadline, stored next to the field in the
// hash body and reported through the listpackex encoding.
func hashTTLCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hexpire", Group: GroupHash, Since: "7.4.0",
			Arity: -6, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHExpire(ctx, "hexpire", "expire") }},
		{Name: "hpexpire", Group: GroupHash, Since: "7.4.0",
			Arity: -6, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHExpire(ctx, "hpexpire", "pexpire") }},
		{Name: "hexpireat", Group: GroupHash, Since: "7.4.0",
			Arity: -6, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHExpire(ctx, "hexpireat", "expireat") }},
		{Name: "hpexpireat", Group: GroupHash, Since: "7.4.0",
			Arity: -6, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHExpire(ctx, "hpexpireat", "pexpireat") }},
		{Name: "hpersist", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHPersist},
		{Name: "httl", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHTTL(ctx, "httl") }},
		{Name: "hpttl", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHTTL(ctx, "hpttl") }},
		{Name: "hexpiretime", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHTTL(ctx, "hexpiretime") }},
		{Name: "hpexpiretime", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHTTL(ctx, "hpexpiretime") }},
	}
}

// parseHashFields reads the "FIELDS numfields field [field ...]" tail starting at
// argv[start]. It returns the field names, or an error string and false when the
// keyword, count or field list do not line up.
func parseHashFields(argv [][]byte, start int) ([][]byte, string, bool) {
	i := start
	if i >= len(argv) || !strings.EqualFold(string(argv[i]), "FIELDS") {
		return nil, "ERR Mandatory keyword FIELDS is missing or not at the right position", false
	}
	i++
	if i >= len(argv) {
		return nil, "ERR value is not an integer or out of range", false
	}
	nf, ok := parseInteger(argv[i])
	if !ok {
		return nil, "ERR value is not an integer or out of range", false
	}
	if nf <= 0 {
		return nil, "ERR Parameter `numFields` should be greater than 0", false
	}
	i++
	fields := argv[i:]
	if int64(len(fields)) != nf {
		return nil, "ERR Parameter `numFields` is more than number of arguments", false
	}
	return fields, "", true
}

// handleHExpire implements HEXPIRE, HPEXPIRE, HEXPIREAT and HPEXPIREAT. It sets a
// per-field deadline subject to the optional NX/XX/GT/LT flag and replies with
// one code per requested field from the §5.8 table.
func handleHExpire(ctx *Ctx, hcmd, mode string) {
	key := ctx.Argv[1]
	tval, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	i := 3
	var cond expireCond
	switch strings.ToUpper(string(ctx.Argv[i])) {
	case "NX":
		cond.nx = true
		i++
	case "XX":
		cond.xx = true
		i++
	case "GT":
		cond.gt = true
		i++
	case "LT":
		cond.lt = true
		i++
	}
	fields, errStr, ok := parseHashFields(ctx.Argv, i)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	now := keyspace.NowMillis()
	when, ok := whenFor(mode, now, tval)
	if !ok {
		ctx.enc().WriteError("ERR invalid expire time in '" + hcmd + "' command")
		return
	}

	codes := make([]int64, len(fields))
	var wrongTyp, noKey, emptied bool
	if !ctx.update(func(db *keyspace.DB) error {
		hf, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if !found {
			noKey = true
			return nil
		}
		changed := false
		for fi, fname := range fields {
			idx := hashFind(hf, fname)
			if idx < 0 {
				codes[fi] = -2
				continue
			}
			cur := hf[idx].ttl
			hasT := cur != 0
			switch {
			case cond.nx && hasT:
				codes[fi] = 0
				continue
			case cond.xx && !hasT:
				codes[fi] = 0
				continue
			case cond.gt && (!hasT || when <= cur):
				codes[fi] = 0
				continue
			case cond.lt && hasT && when >= cur:
				codes[fi] = 0
				continue
			}
			if when <= now {
				hf = append(hf[:idx], hf[idx+1:]...)
				codes[fi] = 2
			} else {
				hf[idx].ttl = when
				codes[fi] = 1
			}
			changed = true
		}
		if changed {
			emptied = len(hf) == 0
			return storeHash(ctx.encLimits(), db, key, hf, hdr)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noKey {
		writeIntArray(ctx, fillInt(len(fields), -2))
		return
	}
	if slices.Contains(codes, int64(1)) {
		ctx.notify(notifyHash, "hexpire", key)
	}
	if slices.Contains(codes, int64(2)) {
		ctx.notify(notifyHash, "hdel", key)
	}
	if emptied {
		ctx.notify(notifyGeneric, "del", key)
	}
	writeIntArray(ctx, codes)
}

// handleHPersist clears the TTL from the named fields. It replies 1 when a TTL
// was removed, 0 when the field was already persistent, and -2 when the field is
// gone, with an all -2 array for a missing key.
func handleHPersist(ctx *Ctx) {
	key := ctx.Argv[1]
	fields, errStr, ok := parseHashFields(ctx.Argv, 2)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	codes := make([]int64, len(fields))
	var wrongTyp, noKey bool
	if !ctx.update(func(db *keyspace.DB) error {
		hf, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if !found {
			noKey = true
			return nil
		}
		changed := false
		for fi, fname := range fields {
			idx := hashFind(hf, fname)
			switch {
			case idx < 0:
				codes[fi] = -2
			case hf[idx].ttl == 0:
				codes[fi] = 0
			default:
				hf[idx].ttl = 0
				codes[fi] = 1
				changed = true
			}
		}
		if changed {
			return storeHash(ctx.encLimits(), db, key, hf, hdr)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noKey {
		writeIntArray(ctx, fillInt(len(fields), -2))
		return
	}
	if slices.Contains(codes, int64(1)) {
		ctx.notify(notifyHash, "hpersist", key)
	}
	writeIntArray(ctx, codes)
}

// handleHTTL implements HTTL, HPTTL, HEXPIRETIME and HPEXPIRETIME. It replies the
// remaining time or absolute deadline per field, -1 for a persistent field, and
// -2 for a missing field, with an all -2 array for a missing key.
func handleHTTL(ctx *Ctx, mode string) {
	key := ctx.Argv[1]
	fields, errStr, ok := parseHashFields(ctx.Argv, 2)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	now := keyspace.NowMillis()
	res := make([]int64, len(fields))
	var wrongTyp, noKey bool
	if !ctx.view(func(db *keyspace.DB) error {
		hf, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if !found {
			noKey = true
			return nil
		}
		for fi, fname := range fields {
			idx := hashFind(hf, fname)
			if idx < 0 {
				res[fi] = -2
				continue
			}
			ttl := hf[idx].ttl
			if ttl == 0 {
				res[fi] = -1
				continue
			}
			switch mode {
			case "httl":
				res[fi] = (ttl - now) / 1000
			case "hpttl":
				res[fi] = ttl - now
			case "hexpiretime":
				res[fi] = ttl / 1000
			default: // hpexpiretime
				res[fi] = ttl
			}
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noKey {
		writeIntArray(ctx, fillInt(len(fields), -2))
		return
	}
	writeIntArray(ctx, res)
}

// storeHash persists a modified hash, deleting the key when no fields remain. It
// keeps the key-level TTL and lets the encoding settle to listpackex or back to
// listpack as field TTLs come and go.
func storeHash(lim encLimits, db *keyspace.DB, key []byte, fields []hashField, hdr keyspace.ValueHeader) error {
	if len(fields) == 0 {
		_, err := db.Delete(key)
		return err
	}
	return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(lim, fields, hdr.Encoding), keepTTL(hdr, true))
}

// writeIntArray replies an array of integers.
func writeIntArray(ctx *Ctx, vals []int64) {
	enc := ctx.enc()
	enc.WriteArrayLen(len(vals))
	for _, v := range vals {
		enc.WriteInteger(v)
	}
}

// fillInt returns a slice of n copies of v.
func fillInt(n int, v int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v
	}
	return out
}
