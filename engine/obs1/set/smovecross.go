package set

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
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
	var logErr error
	t.Do(src, func(cx *shard.Ctx) {
		s, w := registry(cx).lookup(cx, src)
		srcWrong = w
		present = s != nil && s.has(member)
	})
	t.Do(dst, func(cx *shard.Ctx) {
		g := registry(cx)
		d, w := g.lookup(cx, dst)
		dstWrong = w
		if w || srcWrong || !present {
			return
		}
		// Insert at the destination first: the transient member-in-both state
		// is invisible under the barrier, and doing the insert inside the
		// type-check hop saves the fourth hop the naive plan pays.
		created := d == nil
		if created {
			d = newSet(member)
			g.m[string(dst)] = d
		}
		changed := d.add(member)
		g.note(d)
		moved = true
		// Each hop frames its own side, destination first, the same order the
		// seam's cross-group SetMove keeps so a tail cut duplicates the member
		// rather than losing it. A destination that already held the member
		// changed nothing and frames nothing. Tier-two hops emit relaxed-only
		// (the intent Ctx carries no conn), so the frames never gate the reply.
		if changed {
			if err := cx.LogSetAdd(dst, created, [][]byte{member}); err != nil {
				logErr = err
			}
		}
	})
	if moved {
		t.Do(src, func(cx *shard.Ctx) {
			g := registry(cx)
			s, _ := g.lookup(cx, src)
			s.rem(member)
			dropped := s.card() == 0
			if dropped {
				g.drop(src)
			} else {
				g.note(s)
			}
			if err := cx.LogSetRem(src, [][]byte{member}, dropped); err != nil {
				logErr = err
			}
		})
	}
	switch {
	case srcWrong || dstWrong:
		return resp.AppendError(nil, wrongType)
	case logErr != nil:
		return resp.AppendError(nil, logErr.Error())
	case moved:
		return resp.AppendInt(nil, 1)
	default:
		return resp.AppendInt(nil, 0)
	}
}
