package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The cross-shard SMOVE plan (spec 2064/f3/03 section 6.7, spec 2064/f3/11
// section 9.2), the two-key degenerate case of the STORE-form plan: write
// intents on both keys, remove at the source's owner and insert at the
// destination's owner as separate hops, with the membership reply computed at
// the source. The intent barrier (shard.Txn) is the only synchronization; the
// deferral of point traffic on the two keys (shard's txnroute) is what makes
// the two hops one atomic step from every other command's view. The
// co-located case never comes here: dispatch keeps it on the single-shard
// fast path (smove.go), which stays byte-identical to what #602 shipped.

// SmoveCross runs SMOVE source destination member under an acquired
// transaction holding both keys, and returns the finished RESP reply. The
// semantics mirror smove.go exactly, differentially tested against it: both
// types are checked before any mutation, a missing source or absent member
// replies 0, the last member leaving deletes the source, and the destination
// is created on first insert with the same band choice SADD's create path
// makes. src and dst are distinct keys on distinct shards by the dispatch
// check; the same-key case cannot reach here.
func SmoveCross(t *shard.Txn, src, dst, member []byte) []byte {
	var srcWrong, dstWrong, present, moved bool
	t.Do(src, func(cx *shard.Ctx) {
		s, w := registry(cx).operand(cx, src)
		srcWrong = w
		present = s != nil && s.has(member)
	})
	t.Do(dst, func(cx *shard.Ctx) {
		g := registry(cx)
		var dstBuf set
		d, dstHome := g.resolveInto(cx, dst, &dstBuf)
		dstWrong = dstHome == homeString
		if dstWrong || srcWrong || !present {
			return
		}
		// Insert at the destination first: the transient member-in-both state
		// is invisible under the barrier, and doing the insert inside the
		// type-check hop saves the fourth hop the naive plan pays. A missing
		// destination is created inline in the arena, its band chosen from the
		// member's shape exactly as the co-located create does; commit re-embeds a
		// tiny destination or evacuates an escalated one to g.m.
		if d == nil {
			newSetInto(&dstBuf, member)
			d, dstHome = &dstBuf, homeArena
		}
		if d.add(member) {
			cx.NotifyKeyspaceEvent(shard.NotifySet, "sadd", dst)
		}
		g.commit(cx, dst, d, dstHome)
		moved = true
	})
	if moved {
		t.Do(src, func(cx *shard.Ctx) {
			g := registry(cx)
			var srcBuf set
			s, srcHome := g.resolveInto(cx, src, &srcBuf)
			s.rem(member)
			cx.NotifyKeyspaceEvent(shard.NotifySet, "srem", src)
			if s.card() == 0 {
				g.commit(cx, src, s, srcHome)
				cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", src)
			} else {
				g.commit(cx, src, s, srcHome)
			}
		})
	}
	switch {
	case srcWrong || dstWrong:
		return resp.AppendError(nil, wrongType)
	case moved:
		return resp.AppendInt(nil, 1)
	default:
		return resp.AppendInt(nil, 0)
	}
}
