package command

import (
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// hashGetExCommands returns HGETEX and HGETDEL, the read-and-modify hash field
// commands from Redis 7.4.
func hashGetExCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hgetex", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHGetEx},
		{Name: "hgetdel", Group: GroupHash, Since: "7.4.0",
			Arity: -5, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHGetDel},
	}
}

// fieldValue is one entry in a value reply: the bytes, or absent for a missing
// field so the reply writes a null in that slot.
type fieldValue struct {
	val     []byte
	present bool
}

// handleHGetDel reads the named fields and deletes them, replying their values
// before deletion. A missing field or missing key replies a null in that slot.
func handleHGetDel(ctx *Ctx) {
	key := ctx.Argv[1]
	fields, errStr, ok := parseHashFields(ctx.Argv, 2)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}
	out := make([]fieldValue, len(fields))
	var wrongTyp, deleted, emptied bool
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
			return nil
		}
		changed := false
		for fi, fname := range fields {
			idx := hashFind(hf, fname)
			if idx < 0 {
				continue
			}
			out[fi] = fieldValue{val: hf[idx].value, present: true}
			hf = append(hf[:idx], hf[idx+1:]...)
			changed = true
		}
		if changed {
			deleted = true
			emptied = len(hf) == 0
			return storeHash(db, key, hf, hdr)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if deleted {
		ctx.notify(notifyHash, "hdel", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	writeFieldValues(ctx, out)
}

// handleHGetEx reads the named fields and optionally changes their TTL. With
// EX/PX/EXAT/PXAT it sets a deadline, with PERSIST it clears one, and with no
// option it just reads. A missing field or missing key replies a null in that
// slot.
func handleHGetEx(ctx *Ctx) {
	key := ctx.Argv[1]
	now := keyspace.NowMillis()

	i := 2
	mode := "none" // none, persist, or one of the four deadline modes
	var when int64
	switch strings.ToUpper(string(ctx.Argv[i])) {
	case "EX", "PX", "EXAT", "PXAT":
		opt := strings.ToUpper(string(ctx.Argv[i]))
		if i+1 >= len(ctx.Argv) {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		val, ok := parseInteger(ctx.Argv[i+1])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		i += 2
		w, ok := whenFor(getExMode(opt), now, val)
		if !ok {
			ctx.enc().WriteError("ERR invalid expire time in 'hgetex' command")
			return
		}
		mode, when = "set", w
	case "PERSIST":
		mode = "persist"
		i++
	}
	fields, errStr, ok := parseHashFields(ctx.Argv, i)
	if !ok {
		ctx.enc().WriteError(errStr)
		return
	}

	out := make([]fieldValue, len(fields))
	var wrongTyp, setTTL, delField, persisted, emptied bool
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
			return nil
		}
		changed := false
		for fi, fname := range fields {
			idx := hashFind(hf, fname)
			if idx < 0 {
				continue
			}
			out[fi] = fieldValue{val: hf[idx].value, present: true}
			switch mode {
			case "set":
				if when <= now {
					hf = append(hf[:idx], hf[idx+1:]...)
					delField = true
				} else {
					hf[idx].ttl = when
					setTTL = true
				}
				changed = true
			case "persist":
				if hf[idx].ttl != 0 {
					hf[idx].ttl = 0
					persisted = true
					changed = true
				}
			}
		}
		if changed {
			emptied = len(hf) == 0
			return storeHash(db, key, hf, hdr)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if setTTL {
		ctx.notify(notifyHash, "hexpire", key)
	}
	if delField {
		ctx.notify(notifyHash, "hdel", key)
	}
	if persisted {
		ctx.notify(notifyHash, "hpersist", key)
	}
	if emptied {
		ctx.notify(notifyGeneric, "del", key)
	}
	writeFieldValues(ctx, out)
}

// getExMode maps an HGETEX option to the whenFor mode.
func getExMode(opt string) string {
	switch opt {
	case "EX":
		return "expire"
	case "PX":
		return "pexpire"
	case "EXAT":
		return "expireat"
	default: // PXAT
		return "pexpireat"
	}
}

// writeFieldValues replies an array of bulk strings, with a null for any field
// that was absent.
func writeFieldValues(ctx *Ctx, vals []fieldValue) {
	enc := ctx.enc()
	enc.WriteArrayLen(len(vals))
	for _, v := range vals {
		if v.present {
			enc.WriteBulkString(v.val)
		} else {
			enc.WriteNull()
		}
	}
}
