package command

import (
	"github.com/tamnd/aki/keyspace"
)

// hashCommands returns the core hash commands: the set, get, delete and
// inspection family (doc 10 §4). Counters, random and scan are a later slice,
// and the field TTL commands wait for the field-TTL milestone.
func hashCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hset", Group: GroupHash, Since: "2.0.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHSet(ctx, false) }},
		{Name: "hmset", Group: GroupHash, Since: "2.0.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHSet(ctx, true) }},
		{Name: "hsetnx", Group: GroupHash, Since: "2.0.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHSetNX},
		{Name: "hget", Group: GroupHash, Since: "2.0.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHGet},
		{Name: "hmget", Group: GroupHash, Since: "2.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHMGet},
		{Name: "hgetall", Group: GroupHash, Since: "2.0.0",
			Arity: 2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHGetAll},
		{Name: "hdel", Group: GroupHash, Since: "2.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHDel},
		{Name: "hlen", Group: GroupHash, Since: "2.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHLen},
		{Name: "hexists", Group: GroupHash, Since: "2.0.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHExists},
		{Name: "hkeys", Group: GroupHash, Since: "2.0.0",
			Arity: 2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHFields(ctx, true, false) }},
		{Name: "hvals", Group: GroupHash, Since: "2.0.0",
			Arity: 2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleHFields(ctx, false, true) }},
		{Name: "hstrlen", Group: GroupHash, Since: "3.2.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHStrLen},
	}
}

// handleHSet implements HSET and its deprecated HMSET alias. HSET replies the
// number of new fields added; HMSET replies OK.
func handleHSet(ctx *Ctx, asHMSet bool) {
	key := ctx.Argv[1]
	pairs := ctx.Argv[2:]
	if len(pairs)%2 != 0 {
		name := "hset"
		if asHMSet {
			name = "hmset"
		}
		ctx.enc().WriteError("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	var (
		wrongTyp bool
		added    int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		for i := 0; i < len(pairs); i += 2 {
			f, v := pairs[i], pairs[i+1]
			if idx := hashFind(fields, f); idx >= 0 {
				fields[idx].value = v
			} else {
				fields = append(fields, hashField{field: f, value: v})
				added++
			}
		}
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(ctx.encLimits(), fields, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.notify(notifyHash, "hset", key)
	if asHMSet {
		ctx.enc().WriteStatus("OK")
		return
	}
	ctx.enc().WriteInteger(added)
}

// handleHSetNX implements HSETNX: set the field only when it does not exist.
func handleHSetNX(ctx *Ctx) {
	key, field, val := ctx.Argv[1], ctx.Argv[2], ctx.Argv[3]
	var (
		wrongTyp bool
		set      bool
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if hashFind(fields, field) >= 0 {
			return nil
		}
		fields = append(fields, hashField{field: field, value: val})
		set = true
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(ctx.encLimits(), fields, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if set {
		ctx.notify(notifyHash, "hset", key)
		ctx.enc().WriteInteger(1)
	} else {
		ctx.enc().WriteInteger(0)
	}
}

// handleHGet implements HGET: the value of one field, or nil when the field or
// the key is absent.
// hotGetHash tries to decode the hash stored at key from the lock-free hot
// cache. It returns (fields, true) on a hit and (nil, false) on a miss. A
// WRONGTYPE key in the hot cache is treated as a miss so the caller falls back
// to view() which produces the proper WRONGTYPE error.
func hotGetHash(ctx *Ctx, key []byte) ([]hashField, bool) {
	e := ctx.d.engine
	if e == nil {
		return nil, false
	}
	body, hdr, ok := e.viewHotGet(ctx.Conn.DB(), key)
	if !ok {
		return nil, false
	}
	if hdr.Type != keyspace.TypeHash {
		return nil, false
	}
	fields, err := hashDecode(body)
	if err != nil {
		return nil, false
	}
	return dropExpiredFields(fields), true
}

func handleHGet(ctx *Ctx) {
	key, field := ctx.Argv[1], ctx.Argv[2]

	if fields, ok := hotGetHash(ctx, key); ok {
		if idx := hashFind(fields, field); idx >= 0 {
			ctx.enc().WriteBulkString(fields[idx].value)
		} else {
			ctx.enc().WriteNull()
		}
		return
	}

	var (
		wrongTyp bool
		found    bool
		value    []byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fields, hdr, ok, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if idx := hashFind(fields, field); idx >= 0 {
			value = fields[idx].value
			found = true
		}
		return nil
	}) {
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
	ctx.enc().WriteBulkString(value)
}

// handleHMGet implements HMGET: the values of several fields, with nil in each
// position whose field is missing. A missing key replies all nils.
func handleHMGet(ctx *Ctx) {
	key := ctx.Argv[1]
	want := ctx.Argv[2:]

	if fields, ok := hotGetHash(ctx, key); ok {
		enc := ctx.enc()
		enc.WriteArrayLen(len(want))
		for _, f := range want {
			if idx := hashFind(fields, f); idx >= 0 {
				enc.WriteBulkString(fields[idx].value)
			} else {
				enc.WriteNull()
			}
		}
		return
	}

	var (
		wrongTyp bool
		values   = make([][]byte, len(want))
		present  = make([]bool, len(want))
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fields, hdr, ok, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		for i, f := range want {
			if idx := hashFind(fields, f); idx >= 0 {
				values[i] = fields[idx].value
				present[i] = true
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
	enc := ctx.enc()
	enc.WriteArrayLen(len(want))
	for i := range want {
		if present[i] {
			enc.WriteBulkString(values[i])
		} else {
			enc.WriteNull()
		}
	}
}

// handleHGetAll implements HGETALL: every field and value. RESP3 replies a map;
// RESP2 replies a flat field/value array.
func handleHGetAll(ctx *Ctx) {
	key := ctx.Argv[1]

	if fields, ok := hotGetHash(ctx, key); ok {
		enc := ctx.enc()
		enc.WriteMapLen(len(fields))
		for _, f := range fields {
			enc.WriteBulkString(f.field)
			enc.WriteBulkString(f.value)
		}
		return
	}

	var (
		wrongTyp bool
		fields   []hashField
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fs, hdr, ok, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
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
	enc.WriteMapLen(len(fields))
	for _, f := range fields {
		enc.WriteBulkString(f.field)
		enc.WriteBulkString(f.value)
	}
}

// handleHDel implements HDEL: remove the named fields and reply how many were
// removed. The key is deleted when its last field goes.
func handleHDel(ctx *Ctx) {
	key := ctx.Argv[1]
	targets := ctx.Argv[2:]
	var (
		wrongTyp bool
		emptied  bool
		removed  int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		for _, t := range targets {
			if idx := hashFind(fields, t); idx >= 0 {
				fields = append(fields[:idx], fields[idx+1:]...)
				removed++
			}
		}
		if removed == 0 {
			return nil
		}
		if len(fields) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(ctx.encLimits(), fields, hdr.Encoding), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if removed > 0 {
		ctx.notify(notifyHash, "hdel", key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	ctx.enc().WriteInteger(removed)
}

// handleHLen implements HLEN: the field count, or 0 when the key is absent.
func handleHLen(ctx *Ctx) {
	key := ctx.Argv[1]

	if fields, ok := hotGetHash(ctx, key); ok {
		ctx.enc().WriteInteger(int64(len(fields)))
		return
	}

	var (
		wrongTyp bool
		n        int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		n = int64(len(fields))
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleHExists implements HEXISTS: 1 when the field is present, else 0.
func handleHExists(ctx *Ctx) {
	key, field := ctx.Argv[1], ctx.Argv[2]

	if fields, ok := hotGetHash(ctx, key); ok {
		if hashFind(fields, field) >= 0 {
			ctx.enc().WriteInteger(1)
		} else {
			ctx.enc().WriteInteger(0)
		}
		return
	}

	var (
		wrongTyp bool
		exists   bool
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		exists = hashFind(fields, field) >= 0
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if exists {
		ctx.enc().WriteInteger(1)
	} else {
		ctx.enc().WriteInteger(0)
	}
}

// handleHFields implements HKEYS and HVALS: the field names, the values, or both
// in insertion order.
func handleHFields(ctx *Ctx, keys, vals bool) {
	key := ctx.Argv[1]

	if fields, ok := hotGetHash(ctx, key); ok {
		enc := ctx.enc()
		enc.WriteArrayLen(len(fields))
		for _, f := range fields {
			if keys {
				enc.WriteBulkString(f.field)
			} else {
				enc.WriteBulkString(f.value)
			}
		}
		return
	}

	var (
		wrongTyp bool
		fields   []hashField
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fs, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
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
	enc.WriteArrayLen(len(fields))
	for _, f := range fields {
		if keys {
			enc.WriteBulkString(f.field)
		} else if vals {
			enc.WriteBulkString(f.value)
		}
	}
}

// handleHStrLen implements HSTRLEN: the byte length of a field's value, or 0
// when the field or the key is absent.
func handleHStrLen(ctx *Ctx) {
	key, field := ctx.Argv[1], ctx.Argv[2]

	if fields, ok := hotGetHash(ctx, key); ok {
		var n int64
		if idx := hashFind(fields, field); idx >= 0 {
			n = int64(len(fields[idx].value))
		}
		ctx.enc().WriteInteger(n)
		return
	}

	var (
		wrongTyp bool
		n        int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		if idx := hashFind(fields, field); idx >= 0 {
			n = int64(len(fields[idx].value))
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// getHash reads the hash at key and decodes it. The returned header carries the
// type and encoding so callers can check for WRONGTYPE and keep the encoding
// floor. A missing key returns found false with no error.
func getHash(db *keyspace.DB, key []byte) ([]hashField, keyspace.ValueHeader, bool, error) {
	body, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeHash {
		return nil, hdr, true, nil
	}
	fields, err := hashDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return dropExpiredFields(fields), hdr, true, nil
}

// dropExpiredFields removes fields whose per-field TTL has passed. It returns the
// input slice when nothing expired so the common no-TTL case allocates nothing.
func dropExpiredFields(fields []hashField) []hashField {
	now := keyspace.NowMillis()
	any := false
	for _, f := range fields {
		if f.ttl != 0 && f.ttl <= now {
			any = true
			break
		}
	}
	if !any {
		return fields
	}
	live := make([]hashField, 0, len(fields))
	for _, f := range fields {
		if f.ttl != 0 && f.ttl <= now {
			continue
		}
		live = append(live, f)
	}
	return live
}
