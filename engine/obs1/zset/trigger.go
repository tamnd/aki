package zset

import "github.com/tamnd/aki/engine/obs1/shard"

// The zset demotion trigger (spec 2064/f3/06 section 6, mirroring set/trigger.go).
// The worker's demote loop drives one collection quantum per boundary through this
// seam, and the policy of when and what to demote lives here in the zset package
// rather than in the shard, which cannot import zset (the import runs the other
// way). The zset registry is a separate keyspace from the set registry, so it keeps
// its own running footprint and its own trigger; the two compose under the one
// UseDemoter hook, each weighing the other's heap against the shared resident cap
// (dispatch.Demoter, DemoteQuantumOver).
//
// This is the trigger, not the demotion policy. It picks the largest resident zset
// as the victim, the biggest immediate memory win, and drives one bounded rank
// window of the retier-free pack (cold.go). The S3-FIFO coldest-victim policy (doc
// 06 section 4.2, lab 03) is a later refinement over this same seam; it changes only
// which key demoteVictim returns and which rank window demote packs, not the trigger.

// demoteQuantum is the rank window one over-budget boundary sheds from a zset's cold
// (low-rank) end. It bounds a demote's latency to one pack of a fixed member count
// rather than the whole band, so a large zset drains over several boundaries instead
// of a single spike. It is a first-cut lab knob the F9 perf lab tunes; one chunk's
// worth of narrow members is the starting point.
const demoteQuantum = 1 << 12

// DemoteQuantum drives one quantum of zset demotion against the zset registry's own
// resident budget alone. It is the single-type entry the zset trigger's own tests
// use; a shard that also runs other native collections composes the combined budget
// through DemoteQuantumOver instead.
func DemoteQuantum(cx *shard.Ctx) int { return DemoteQuantumOver(cx, 0) }

// DemoteQuantumOver drives one quantum of zset demotion when the shard's resident
// footprint, the arena plus this registry's own owner-local heap plus the extra
// other collection registries report, sits past the store's resident cap. The extra
// carries the set registry's heap so the one resident cap bounds the combined RSS:
// the zset sheds nothing while the arena plus both heaps fits and one quantum once
// it does not. It returns the members it moved to the cold tier, zero when there is
// no cold tier, no registry yet, the shard is under the combined budget, or every
// zset is already fully cold. Owner goroutine only, called at a demote boundary
// where no handler holds an arena address.
func DemoteQuantumOver(cx *shard.Ctx, extra uint64) int {
	g, ok := cx.ZColl.(*reg)
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
	return g.demote(cx, []byte(key), demoteQuantum)
}

// demoteVictim picks the demotable zset with the largest resident footprint, the
// key one quantum sheds the most bytes from. It skips zsets with nothing left to
// demote (a listpack band, or a native band already fully cold), so a fully-cold
// zset never wins the pick and stalls the loop while resident zsets stay hot. It
// returns the empty string when no zset can shed a byte, which stops the quantum.
// O(keys) per over-budget boundary, off the steady no-pressure path.
func (g *reg) demoteVictim() string {
	var victim string
	var best uint64
	for k, z := range g.m {
		if !z.demotable() {
			continue
		}
		if nb := z.residentBytes(); nb > best {
			best = nb
			victim = k
		}
	}
	return victim
}

// demotable reports whether the zset still holds resident member bytes a demote can
// move to the cold tier: a native band whose slab still carries live bytes. The
// listpack band sits below one chunk's worth and never demotes, and a native band
// whose every member has already left for the cold tier has nothing left to shed
// until fresh members land in it.
func (z *zset) demotable() bool {
	return z.enc == encSkiplist && z.nat.hasResident()
}
