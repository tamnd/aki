package set

import "github.com/tamnd/aki/engine/obs1/shard"

// The set demotion trigger (spec 2064/f3/06 section 6): the worker's demote loop
// drives one collection quantum per boundary through this seam, and the policy of
// when and what to demote lives here in the set package rather than in the shard,
// which cannot import set (the import runs the other way). The shard registers
// DemoteQuantum through Runtime.UseDemoter and calls it every demote boundary
// beside the arena's own MaybeDemote; the function self-gates, so a store with no
// cold tier or a shard under its resident budget pays one predictable branch and
// demotes nothing (the L9 zero-delta contract).
//
// This is the trigger, not the demotion policy. It picks the largest resident set
// as the victim, the biggest immediate memory win, and drives one bounded quantum
// of the retier-free pack (cold.go). The S3-FIFO probation-and-ghost policy that
// picks the coldest victim by heat (doc 06 section 4.2, lab 03) is a later
// refinement over this same seam; it changes only which key demoteVictim returns,
// not the trigger or the pack.

// DemoteQuantum drives one quantum of set demotion against the set registry's own
// resident budget alone. It is the single-type entry a set-only deployment and the
// set trigger's own tests use; a shard that also runs other native collections
// composes the combined budget through DemoteQuantumOver instead.
func DemoteQuantum(cx *shard.Ctx) int { return DemoteQuantumOver(cx, 0) }

// DemoteQuantumOver drives one quantum of collection demotion when the shard's
// resident footprint, the arena plus this registry's own owner-local heap plus the
// extra other collection registries report, sits past the store's resident cap. The
// extra is what makes the one resident cap a true RSS bound across every native
// collection type: the set and the zset each weigh their own heap against the budget
// only after adding the other's, so neither type sheds a byte while the combined
// footprint fits and both do once it does not. It returns the members it moved to
// the cold tier, zero when there is no cold tier, no registry yet, the shard is
// under the combined budget, or every set is already fully cold. Owner goroutine
// only, called at a demote boundary where no handler holds an arena address.
func DemoteQuantumOver(cx *shard.Ctx, extra uint64) int {
	g, ok := cx.Coll.(*reg)
	if !ok || !g.acctOn {
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

// demoteVictim picks the demotable set with the largest resident footprint, the
// key one quantum sheds the most bytes from. It skips sets with nothing left to
// demote (inline bands, and native or partitioned sets already fully cold), so a
// fully-cold set never wins the pick and stalls the loop while resident sets stay
// hot. It returns the empty string when no set can shed a byte, which stops the
// quantum. O(keys) per over-budget boundary, off the steady no-pressure path.
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

// demotable reports whether the set still holds resident member bytes a demote can
// move to the cold tier: a native set with a live slab, or a partitioned set with
// at least one sub-table still holding one. The inline bands (intset, listpack) sit
// below one chunk's worth and never demote, and a set whose every slab has already
// been freed has nothing left to shed until fresh members land in it.
func (s *set) demotable() bool {
	switch s.enc {
	case encHashtable:
		return s.ht.slab != nil
	case encPartitioned:
		for _, h := range s.part.parts {
			if h.slab != nil {
				return true
			}
		}
	}
	return false
}
