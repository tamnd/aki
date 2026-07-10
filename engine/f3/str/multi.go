package str

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// The fan-out sub-command handlers: each one executes a multi-key command's
// per-shard slice on the owner and answers a partial in the fan encoding
// instead of RESP. Per-key atomicity only, which is the tier-one contract
// (doc 03 section 6.1): keys on other shards are untouched by anything this
// handler does.

// MGetShard answers an MGET sub-command: every argument but the last is a key,
// the last is the positions blob the gather side reads back off the node. The
// partial is one length-prefixed entry per key, in sub-command order. A
// chunked value is materialized whole into the partial here; MGET does not
// stream in M0, only GET does, and the copy is bounded by the value cap.
func MGetShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	part := cx.Aux[:0]
	for _, key := range args[:len(args)-1] {
		v, ok := cx.St.GetString(key, cx.NowMs, cx.Val)
		cx.Val = v
		part = shard.AppendFanValue(part, v, ok)
	}
	cx.Aux = part
	r.Raw(part)
}

// MSetShard answers an MSET sub-command of key/value pairs. The partial is
// empty on success; on a store error it carries the wire message and the
// gather side reports the first one. Pairs before a failing pair stay
// written: per-key atomicity, not command atomicity.
func MSetShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	for i := 0; i+1 < len(args); i += 2 {
		if err := cx.St.SetString(args[i], args[i+1], cx.NowMs, 0, false); err != nil {
			r.FanErrString(storeErr(err))
			return
		}
	}
	r.FanOK()
}

// DelShard answers a DEL or UNLINK sub-command: the partial is this shard's
// deleted-key count. UNLINK shares the handler because reclamation is already
// owner-local and immediate here; there is no background free to hand off to.
func DelShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var n int64
	for _, key := range args {
		if cx.St.Del(key, cx.NowMs) {
			n++
		}
	}
	r.FanCount(n)
}

// ExistsShard answers an EXISTS sub-command: the partial counts every key
// argument that exists, duplicates included, which is the Redis EXISTS
// contract. Duplicate keys hash to the same shard, so per-shard counting
// composes exactly.
func ExistsShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var n int64
	for _, key := range args {
		if cx.St.Exists(key, cx.NowMs) {
			n++
		}
	}
	r.FanCount(n)
}
