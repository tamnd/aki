package hash

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The hash command surface over the inline and native bands (spec 2064/f3/10
// section 7). Every handler runs on its shard's owner goroutine, so the registry
// and every hash in it are plain single-owner state. Array replies (HMGET) are
// built in the shard scratch (cx.Aux) with the resp emitters and handed over
// whole through Reply.Raw, the same one-pass shape the set surface uses.

// Hset answers HSET key field value [field value ...]: write each pair, reply the
// count of newly created fields. A missing key creates the hash; a key the string
// store owns answers WRONGTYPE; an odd pair tail is an arity error. HSET preserves
// an existing field's TTL, which is trivially true this slice since no field TTL
// machinery exists yet (spec 2064/f3/10 section 7.1).
func Hset(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args)%2 != 1 {
		// args is key followed by field-value pairs, so a well-formed tail has an
		// odd argument count; anything else is the wrong number of arguments.
		r.Err("ERR wrong number of arguments for 'hset' command")
		return
	}
	g, h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var added int64
	for i := 1; i < len(args); i += 2 {
		if h.set(args[i], args[i+1]) {
			added++
		} else {
			// Overwriting a field with HSET drops its TTL (verified against Redis 8.8:
			// HSET clears the field TTL, HINCRBY keeps it). A no-op on a field without
			// a TTL, and O(1) on a hash that never set one.
			h.clearFieldExp(args[i])
		}
		// Log every pair, new or overwritten: a replay must reproduce the value the
		// field now holds either way.
		logSet(cx, args[0], args[i], args[i+1])
	}
	g.note(h)
	r.Int(added)
}

// Hmset answers HMSET key field value [field value ...]: the same write as HSET
// with a +OK reply instead of the new-field count (spec 2064/f3/10 section 7.1).
func Hmset(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args)%2 != 1 {
		r.Err("ERR wrong number of arguments for 'hmset' command")
		return
	}
	g, h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	for i := 1; i < len(args); i += 2 {
		if !h.set(args[i], args[i+1]) {
			// HMSET clears an overwritten field's TTL, the same as HSET.
			h.clearFieldExp(args[i])
		}
		logSet(cx, args[0], args[i], args[i+1])
	}
	g.note(h)
	r.Status("OK")
}

// Hsetnx answers HSETNX key field value: set the field and reply 1 when it is new,
// reply 0 without overwriting when it already exists (spec 2064/f3/10 section 7.1).
func Hsetnx(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	// A refused HSETNX means the field already exists, which can only happen on a
	// hash that already held fields, so getOrCreate never leaves a stray empty
	// hash behind here: a freshly created hash is empty and always accepts the set.
	g, h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h.setNX(args[1], args[2]) {
		logSet(cx, args[0], args[1], args[2])
		g.note(h)
		r.Int(1)
		return
	}
	r.Int(0)
}

// Hget answers HGET key field: the value bulk, or nil when the field or the key is
// absent. An empty-string value is a $0 bulk, distinct from nil (spec 2064/f3/10
// section 7.2).
func Hget(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Null()
		return
	}
	if v, ok := h.get(args[1]); ok {
		r.Bulk(v)
		return
	}
	r.Null()
}

// Hmget answers HMGET key field [field ...]: an array exactly as long as the
// request, a nil per absent field, repeated fields answered repeatedly. A missing
// key answers all-nil (spec 2064/f3/10 section 7.2).
func Hmget(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	fields := args[1:]
	out := resp.AppendArrayHeader(cx.Aux[:0], len(fields))
	for _, f := range fields {
		if h != nil {
			if v, ok := h.get(f); ok {
				out = resp.AppendBulk(out, v)
				continue
			}
		}
		out = resp.AppendNull(out)
	}
	cx.Aux = out
	r.Raw(out)
}

// Hgetdel answers HGETDEL key FIELDS numfields field [field ...]: return each
// field's value, nil when it is absent, and delete the present ones in the same
// step. The key is dropped when its last field leaves. Each value is copied into
// the reply before its field is deleted, so a listpackex record freed by the
// delete cannot alter bytes already framed (spec 2064/f3/10 section 7.4).
func Hgetdel(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	fields, _, ferr := parseFieldsClause("hgetdel", args[1:], false)
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
	for _, f := range fields {
		if v, ok := h.get(f); ok {
			out = resp.AppendBulk(out, v)
			h.del(f)
			logDelField(cx, args[0], f)
		} else {
			out = resp.AppendNull(out)
		}
	}
	cx.Aux = out
	if h.card() == 0 {
		g.drop(args[0])
	} else {
		g.note(h)
	}
	r.Raw(out)
}

// Hexists answers HEXISTS key field: 1 when present, 0 otherwise.
func Hexists(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h != nil && h.has(args[1]) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// Hstrlen answers HSTRLEN key field: the value length, or 0 when the field or the
// key is absent (spec 2064/f3/10 section 7.2).
func Hstrlen(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Int(0)
		return
	}
	r.Int(int64(h.strlen(args[1])))
}

// Hlen answers HLEN key: the field count, 0 when absent, an O(1) header read on
// both bands (spec 2064/f3/10 section 7.5).
func Hlen(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Int(0)
		return
	}
	r.Int(int64(h.card()))
}

// Hdel answers HDEL key field [field ...]: remove each field, reply the count
// removed, and delete the key when the last field leaves (spec 2064/f3/10 section
// 7.4).
func Hdel(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Int(0)
		return
	}
	var removed int64
	for _, f := range args[1:] {
		if h.del(f) {
			removed++
			logDelField(cx, args[0], f)
		}
	}
	if h.card() == 0 {
		g.drop(args[0])
	} else {
		g.note(h)
	}
	r.Int(removed)
}

// getOrCreate returns the hash for key, creating an empty one when the key is
// absent, and the registry the caller reconciles the footprint into. wrong is true
// when the string store owns the key, which the caller answers with WRONGTYPE.
// Callers only reach it on a write that adds at least one field, so a created hash
// never stays empty; the caller notes it into the resident total before returning.
func getOrCreate(cx *shard.Ctx, key []byte) (g *reg, h *hash, wrong bool) {
	g = registry(cx)
	// The live funnel drops a hash whose key-level deadline has passed or whose
	// fields have all expired, so a write onto an expired key builds a fresh hash
	// below (a new listpack with no key TTL and no lingering listpackex), never
	// resurrects the stale one. This is the create-path hazard the rollout plan
	// calls out, closed by the same funnel every read routes through.
	if h = g.live(cx, key); h != nil {
		return g, h, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return g, nil, true
	}
	h = newHash()
	g.install(cx, key, h)
	return g, h, false
}
