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
// Each read is a view under the store.GetView lifetime rule, consumed by
// AppendFanValue's copy before the next read reuses the store scratch.
func MGetShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	part := cx.Aux[:0]
	for _, key := range args[:len(args)-1] {
		v, ok := cx.St.GetView(key, cx.NowMs)
		part = shard.AppendFanValue(part, v, ok)
	}
	cx.Aux = part
	r.Raw(part)
}

// MSetShard answers an MSET sub-command of key/value pairs. The partial is
// empty on success; on a store error it carries the wire message and the
// gather side reports the first one. Pairs before a failing pair stay
// written: per-key atomicity, not command atomicity.
//
// Under memory pressure a pair that cannot allocate parks the sub-command
// instead of dropping it (block-not-drop, F9): ParkFullAt records the failing
// pair's index and the worker retries the sub-command when a drain frees room,
// resuming at that pair (ResumeIndex) so the committed prefix is not re-applied.
// The parked slot writes no partial; the retry writes FanOK on success or, past a
// genuine stall, the OOM partial the worker frames. Only store.ErrFull parks; any
// other store error still reports through the partial at once.
func MSetShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	for i := cx.ResumeIndex(); i+1 < len(args); i += 2 {
		if err := cx.St.SetString(args[i], args[i+1], cx.NowMs, 0, false); err != nil {
			if cx.ParkFullAt(err, i) {
				return
			}
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
