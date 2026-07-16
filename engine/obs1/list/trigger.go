package list

import "github.com/tamnd/aki/engine/obs1/shard"

// The list demotion trigger (spec 2064/f3/06 section 6, mirroring set/trigger.go and
// zset/trigger.go). The worker's demote loop drives one collection quantum per
// boundary through this seam, and the policy of when and what to demote lives here in
// the list package rather than in the shard, which cannot import list (the import
// runs the other way). The list registry hangs off the shared regs map keyed by the
// shard's store, a separate keyspace from the set and zset registries, so it keeps
// its own running footprint and its own trigger; the three compose under the one
// UseDemoter hook, each weighing the other two heaps against the shared resident cap
// (dispatch.Demoter, DemoteQuantumOver).
//
// This is the trigger, not the demotion policy. It picks the largest resident list
// as the victim, the biggest immediate memory win, and drives one bounded quantum of
// the whole-chunk interior demote (cold.go). The interior-only policy makes the list
// the safest of the three to shed, since it provably never touches a hot end; the
// S3-FIFO coldest-victim refinement (doc 06 section 4.2, lab 03) is a later change
// over this same seam, altering only which key demoteVictim returns, not the trigger.

// DemoteQuantum drives one quantum of list demotion against the list registry's own
// resident budget alone. It is the single-type entry the list trigger's own tests
// use; a shard that also runs the other native collections composes the combined
// budget through DemoteQuantumOver instead.
func DemoteQuantum(cx *shard.Ctx) int { return DemoteQuantumOver(cx, 0) }

// DemoteQuantumOver drives one quantum of list demotion when the shard's resident
// footprint, the arena plus this registry's own owner-local heap plus the extra the
// other collection registries report, sits past the store's resident cap. The extra
// carries the set and zset registries' heaps so the one resident cap bounds the
// combined RSS: the list sheds nothing while the arena plus every collection heap
// fits and one quantum once it does not. It returns the elements it moved to the cold
// tier, zero when there is no cold tier, no registry yet, the shard is under the
// combined budget, or every list is already fully cold in the interior. It loads the
// registry without building one, so a shard that never ran a list command stays inert
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

// demoteVictim picks the demotable list with the largest resident footprint, the key
// one quantum sheds the most bytes from. It skips lists with nothing left to demote
// (an inline listpack band, and a native band whose interior is already fully cold),
// so a fully-cold list never wins the pick and stalls the loop while resident lists
// stay hot. It returns the empty string when no list can shed a byte, which stops the
// quantum. O(keys) per over-budget boundary, off the steady no-pressure path.
func (g *reg) demoteVictim() string {
	var victim string
	var best uint64
	for k, l := range g.m {
		if !l.demotable() {
			continue
		}
		if nb := l.residentBytes(); nb > best {
			best = nb
			victim = k
		}
	}
	return victim
}

// demotable reports whether the list still holds a resident interior chunk a demote
// can move to the cold tier. The inline listpack band sits below one chunk's worth
// and never demotes, and a native band whose every interior chunk has already left
// for the cold tier has nothing left to shed until fresh elements land in the
// interior. The two end margins stay resident by policy, so a list of only margin
// chunks is not demotable either.
func (l *list) demotable() bool {
	return l.nat != nil && l.nat.hasResidentInterior()
}
