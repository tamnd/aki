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
	h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var added int64
	for i := 1; i < len(args); i += 2 {
		if h.set(args[i], args[i+1]) {
			added++
		}
	}
	r.Int(added)
}

// Hmset answers HMSET key field value [field value ...]: the same write as HSET
// with a +OK reply instead of the new-field count (spec 2064/f3/10 section 7.1).
func Hmset(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args)%2 != 1 {
		r.Err("ERR wrong number of arguments for 'hmset' command")
		return
	}
	h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	for i := 1; i < len(args); i += 2 {
		h.set(args[i], args[i+1])
	}
	r.Status("OK")
}

// Hsetnx answers HSETNX key field value: set the field and reply 1 when it is new,
// reply 0 without overwriting when it already exists (spec 2064/f3/10 section 7.1).
func Hsetnx(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	// A refused HSETNX means the field already exists, which can only happen on a
	// hash that already held fields, so getOrCreate never leaves a stray empty
	// hash behind here: a freshly created hash is empty and always accepts the set.
	h, wrong := getOrCreate(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if h.setNX(args[1], args[2]) {
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
		}
	}
	if h.card() == 0 {
		g.drop(args[0])
	}
	r.Int(removed)
}

// getOrCreate returns the hash for key, creating an empty one when the key is
// absent. wrong is true when the string store owns the key, which the caller
// answers with WRONGTYPE. Callers only reach it on a write that adds at least one
// field, so a created hash never stays empty.
func getOrCreate(cx *shard.Ctx, key []byte) (h *hash, wrong bool) {
	g := registry(cx)
	if h = g.m[string(key)]; h != nil {
		return h, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	h = newHash()
	g.m[string(key)] = h
	return h, false
}
