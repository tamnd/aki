package stream

import "github.com/tamnd/aki/engine/obs1/shard"

// The stream demotion trigger (spec 2064/f3/06 section 6, mirroring set/trigger.go,
// zset/trigger.go, list/trigger.go, and hash/trigger.go). The worker's demote loop
// drives one collection quantum per boundary through this seam, and the policy of when
// and what to demote lives here in the stream package rather than in the shard, which
// cannot import stream (the import runs the other way). The stream registry hangs off
// the shared regs map keyed by the shard's store, a separate keyspace from the set,
// zset, list, and hash registries, so it keeps its own running footprint and its own
// trigger; the five compose under the one UseDemoter hook, each weighing the other
// four heaps against the shared resident cap (dispatch.Demoter, DemoteQuantumOver).
//
// This is the trigger, not the demotion policy. It picks the largest resident stream
// as the victim, the biggest immediate memory win, and drives one bounded quantum of
// the whole-block demote (cold.go), which spills the oldest front blocks straight to
// the cold region and keeps demoteTailMargin newest blocks resident so a fresh XADD
// and an XREAD $ never pread. The S3-FIFO coldest-victim refinement (doc 06 section
// 4.2, lab 03) is a later change over this same seam, altering only which key
// demoteVictim returns, not the trigger.

// DemoteQuantum drives one quantum of stream demotion against the stream registry's
// own resident budget alone. It is the single-type entry the stream trigger's own
// tests use; a shard that also runs the other native collections composes the combined
// budget through DemoteQuantumOver instead.
func DemoteQuantum(cx *shard.Ctx) int { return DemoteQuantumOver(cx, 0) }

// DemoteQuantumOver drives one quantum of stream demotion when the shard's resident
// footprint, the arena plus this registry's own owner-local heap plus the extra the
// other collection registries report, sits past the store's resident cap. The extra
// carries the set, zset, list, and hash registries' heaps so the one resident cap
// bounds the combined RSS: the stream sheds nothing while the arena plus every
// collection heap fits and one quantum once it does not. It returns the entries it
// moved to the cold tier, zero when there is no cold tier, no registry yet, the shard
// is under the combined budget, or every stream is already fully cold. It loads the
// registry without building one, so a shard that never ran a stream command stays inert
// at the boundary. Owner goroutine only, called at a demote boundary where no handler
// holds an arena address.
func DemoteQuantumOver(cx *shard.Ctx, extra uint64) int {
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	g := v.(*reg)
	if !g.acctOn {
		return 0
	}
	if !cx.St.ResidentOverBy(g.resident + extra) {
		return 0
	}
	key := g.demoteVictim()
	if key == "" {
		return 0
	}
	return g.demote(cx, []byte(key))
}

// demote sheds one quantum of the stream at key into the cold region and returns the
// blocks it moved, reconciling the running total on a shed. It is the reg-level wrapper
// the trigger drives: the stream method (cold.go) does the spill and drops the block
// blobs from resBlob, and this posts the delta into the registry's resident sum so the
// next boundary reads the shed footprint. It returns 0 when the key is absent or the
// pass sheds nothing (a fully-cold front, an inline band, or a refused append).
func (g *reg) demote(cx *shard.Ctx, key []byte) int {
	s := g.m[string(key)]
	if s == nil {
		return 0
	}
	n := s.demote(cx.St, key)
	if n > 0 {
		g.note(s)
	}
	return n
}

// demoteVictim picks the demotable stream with the largest resident footprint, the key
// one quantum sheds the most bytes from. It skips streams with nothing left to demote
// (an inline band, and a native band whose every sheddable front block is already
// cold), so a fully-cold stream never wins the pick and stalls the loop while resident
// streams stay hot. It returns the empty string when no stream can shed a byte, which
// stops the quantum. O(keys) per over-budget boundary, off the steady no-pressure path.
func (g *reg) demoteVictim() string {
	var victim string
	var best uint64
	for k, s := range g.m {
		if !s.demotable() {
			continue
		}
		if nb := s.residentBytes(); nb > best {
			best = nb
			victim = k
		}
	}
	return victim
}

// demotable reports whether the stream still holds a resident front block a demote can
// spill to the cold tier. The inline band sits in one block below a chunk's worth and
// never demotes, and a native band whose every front block ahead of the tail margin is
// already cold has nothing left to shed until a fresh XADD seals a new block (the
// newest demoteTailMargin blocks stay resident by design, so its footprint alone would
// keep drawing the pick). Only a native band with a resident sealed front block is
// demotable, mirroring the demote pass's own skip of cold and empty handles (cold.go).
func (s *stream) demotable() bool {
	if s.kind != bandNative {
		return false
	}
	limit := len(s.blocks) - demoteTailMargin
	for i := 0; i < limit; i++ {
		b := s.blocks[i]
		if !b.cold() && len(b.blob) != 0 {
			return true
		}
	}
	return false
}
