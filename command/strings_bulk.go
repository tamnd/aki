package command

import (
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// defProtoMaxBulkLen is the default largest string value aki stores, matching
// Redis's default proto-max-bulk-len of 512 MiB.
const defProtoMaxBulkLen = 512 << 20

// protoMaxBulkLen returns the current proto-max-bulk-len, the size cap APPEND,
// SETRANGE, SETBIT, and BITFIELD enforce when growing a value. It tracks the
// config directive so CONFIG SET proto-max-bulk-len changes it without a restart.
func (d *Dispatcher) protoMaxBulkLen() int64 {
	return d.confInt("proto-max-bulk-len", defProtoMaxBulkLen)
}

// handleMSet implements MSET key value [key value ...]: set every pair, always
// returning OK. It needs an even number of key/value arguments; an odd count is
// a syntax error.
func handleMSet(ctx *Ctx) {
	pairs := ctx.Argv[1:]
	if len(pairs)%2 != 0 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	if ctx.update(func(db *keyspace.DB) error {
		for i := 0; i < len(pairs); i += 2 {
			key, val := pairs[i], pairs[i+1]
			if err := db.Set(key, val, keyspace.TypeString, stringEncoding(val), -1); err != nil {
				return err
			}
		}
		return nil
	}) {
		for i := 0; i < len(pairs); i += 2 {
			ctx.notify(notifyString, "set", pairs[i])
		}
		ctx.Conn.WriteRaw(resp.ReplyOK)
	}
}

// handleMSetNX implements MSETNX key value [key value ...]: set every pair only
// if none of the keys already exist. It returns 1 when all were set and 0 when
// at least one existed, in which case nothing is written.
func handleMSetNX(ctx *Ctx) {
	pairs := ctx.Argv[1:]
	if len(pairs)%2 != 0 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'msetnx' command")
		return
	}
	var allSet bool
	if ctx.update(func(db *keyspace.DB) error {
		for i := 0; i < len(pairs); i += 2 {
			exists, err := db.Exists(pairs[i])
			if err != nil {
				return err
			}
			if exists {
				return nil
			}
		}
		allSet = true
		for i := 0; i < len(pairs); i += 2 {
			key, val := pairs[i], pairs[i+1]
			if err := db.Set(key, val, keyspace.TypeString, stringEncoding(val), -1); err != nil {
				return err
			}
		}
		return nil
	}) {
		if allSet {
			for i := 0; i < len(pairs); i += 2 {
				ctx.notify(notifyString, "set", pairs[i])
			}
		}
		ctx.enc().WriteInteger(boolToInt(allSet))
	}
}

// handleMGet implements MGET key [key ...]: return one array element per key,
// the value for a string key and null for a missing key or a key holding any
// other type. MGET never returns WRONGTYPE.
func handleMGet(ctx *Ctx) {
	keys := ctx.Argv[1:]
	values := make([][]byte, len(keys))
	present := make([]bool, len(keys))
	if !ctx.view(func(db *keyspace.DB) error {
		for i, k := range keys {
			b, hdr, found, err := db.Get(k)
			if err != nil {
				return err
			}
			if found && hdr.Type == keyspace.TypeString {
				values[i], present[i] = b, true
			}
		}
		return nil
	}) {
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(keys))
	for i := range keys {
		if present[i] {
			enc.WriteBulkString(values[i])
		} else {
			enc.WriteNull()
		}
	}
}

// handleAppend implements APPEND key value: append to the existing string,
// creating an empty one first when the key is absent. The result encoding is
// always raw. It returns the new length.
func handleAppend(ctx *Ctx) {
	key, val := ctx.Argv[1], ctx.Argv[2]
	var (
		wrongTyp bool
		tooBig   bool
		newLen   int
	)
	done := ctx.update(func(db *keyspace.DB) error {
		old, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		if int64(len(old)+len(val)) > ctx.d.protoMaxBulkLen() {
			tooBig = true
			return nil
		}
		body := make([]byte, 0, len(old)+len(val))
		body = append(body, old...)
		body = append(body, val...)
		newLen = len(body)
		ttl := keepTTL(hdr, found)
		return db.Set(key, body, keyspace.TypeString, keyspace.EncRaw, ttl)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if tooBig {
		ctx.enc().WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}
	ctx.notify(notifyString, "append", key)
	ctx.enc().WriteInteger(int64(newLen))
}

// handleStrlen implements STRLEN key: the byte length of the string value, 0 for
// a missing key, WRONGTYPE for a non-string key.
func handleStrlen(ctx *Ctx) {
	key := ctx.Argv[1]
	var (
		wrongTyp bool
		n        int
	)
	if !ctx.view(func(db *keyspace.DB) error {
		b, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		n = len(b)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(int64(n))
}

// handleSetRange implements SETRANGE key offset value: overwrite bytes starting
// at offset, zero-padding with NUL bytes when the offset is past the current
// length. The result encoding is always raw. It returns the new length.
func handleSetRange(ctx *Ctx) {
	key, val := ctx.Argv[1], ctx.Argv[3]
	offset, ok := parseInteger(ctx.Argv[2])
	if !ok || offset < 0 {
		ctx.enc().WriteError("ERR offset is not an integer or out of range")
		return
	}
	if offset+int64(len(val)) > ctx.d.protoMaxBulkLen() {
		ctx.enc().WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}
	var (
		wrongTyp bool
		wrote    bool
		newLen   int
	)
	done := ctx.update(func(db *keyspace.DB) error {
		old, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		// An empty value never creates or extends a key; it just reports the
		// current length, matching Redis.
		if len(val) == 0 {
			newLen = len(old)
			return nil
		}
		size := max(int(offset)+len(val), len(old))
		body := make([]byte, size)
		copy(body, old)
		copy(body[offset:], val)
		newLen = len(body)
		wrote = true
		ttl := keepTTL(hdr, found)
		return db.Set(key, body, keyspace.TypeString, keyspace.EncRaw, ttl)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if wrote {
		ctx.notify(notifyString, "setrange", key)
	}
	ctx.enc().WriteInteger(int64(newLen))
}

// handleGetRange implements GETRANGE key start end (and its SUBSTR alias): the
// inclusive substring between byte offsets start and end, with negative indices
// counting from the end. An out-of-range or inverted range yields an empty
// string, never null.
func handleGetRange(ctx *Ctx) {
	key := ctx.Argv[1]
	start, ok1 := parseInteger(ctx.Argv[2])
	end, ok2 := parseInteger(ctx.Argv[3])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		out      []byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		b, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		out = sliceRange(b, start, end)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteBulkString(out)
}

// sliceRange returns the inclusive [start, end] byte range of b using Redis's
// index rules: negative indices count from the end, start clamps up to 0, end
// clamps down to the last byte, and an empty or inverted range gives an empty
// slice.
func sliceRange(b []byte, start, end int64) []byte {
	n := int64(len(b))
	if n == 0 {
		return nil
	}
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n {
		return nil
	}
	return b[start : end+1]
}

// keepTTL returns the absolute deadline that preserves a key's current TTL on an
// in-place modify like APPEND or SETRANGE, or -1 when the key has no TTL.
func keepTTL(hdr keyspace.ValueHeader, found bool) int64 {
	if found && hdr.HasTTL() {
		return hdr.TTLms
	}
	return -1
}
