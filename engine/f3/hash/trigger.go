package hash

import "github.com/tamnd/aki/engine/f3/shard"

// The hash demotion trigger (spec 2064/f3/06 section 6, mirroring set/trigger.go,
// zset/trigger.go, and list/trigger.go). The worker's demote loop drives one
// collection quantum per boundary through this seam, and the policy of when and what
// to demote lives here in the hash package rather than in the shard, which cannot
// import hash (the import runs the other way). The hash registry hangs off the shared
// regs map keyed by the shard's store, a separate keyspace from the set, zset, and
// list registries, so it keeps its own running footprint and its own trigger; the
// four compose under the one UseDemoter hook, each weighing the other three heaps
// against the shared resident cap (dispatch.Demoter, DemoteQuantumOver).
//
// This is the trigger, not the demotion policy. It picks the largest resident hash as
// the victim, the biggest immediate memory win, and drives one bounded quantum of the
// value-shedding demote (cold.go), which packs the field-and-value pair to disk but
// keeps the field bytes resident so the probe stays zero-pread. The S3-FIFO
// coldest-victim refinement (doc 06 section 4.2, lab 03) is a later change over this
// same seam, altering only which key demoteVictim returns, not the trigger.

// DemoteQuantum drives one quantum of hash demotion against the hash registry's own
// resident budget alone. It is the single-type entry the hash trigger's own tests
// use; a shard that also runs the other native collections composes the combined
// budget through DemoteQuantumOver instead.
func DemoteQuantum(cx *shard.Ctx) int { return DemoteQuantumOver(cx, 0) }

// DemoteQuantumOver drives one quantum of hash demotion when the shard's resident
// footprint, the arena plus this registry's own owner-local heap plus the extra the
// other collection registries report, sits past the store's resident cap. The extra
// carries the set, zset, and list registries' heaps so the one resident cap bounds
// the combined RSS: the hash sheds nothing while the arena plus every collection heap
// fits and one quantum once it does not. It returns the values it moved to the cold
// tier, zero when there is no cold tier, no registry yet, the shard is under the
// combined budget, or every hash is already fully cold. It loads the registry without
// building one, so a shard that never ran a hash command stays inert at the boundary.
// Owner goroutine only, called at a demote boundary where no handler holds an arena
// address.
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

// demoteVictim picks the demotable hash with the largest resident footprint, the key
// one quantum sheds the most bytes from. It skips hashes with nothing left to demote
// (an inline listpack band, and a native band whose every value is already cold), so
// a fully-cold hash never wins the pick and stalls the loop while resident hashes stay
// hot. It returns the empty string when no hash can shed a byte, which stops the
// quantum. O(keys) per over-budget boundary, off the steady no-pressure path.
func (g *reg) demoteVictim() string {
	var victim string
	var best uint64
	for k, h := range g.m {
		if !h.demotable() {
			continue
		}
		if nb := h.residentBytes(); nb > best {
			best = nb
			victim = k
		}
	}
	return victim
}

// demotable reports whether the hash still holds a resident value a demote can move
// to the cold tier. The inline listpack band sits below one chunk's worth and never
// demotes, and a native band whose every value has already left for the cold tier has
// nothing left to shed until a fresh field lands (the field bytes stay resident by
// design, so its footprint alone would keep drawing the pick). Only the native band
// with at least one resident value is demotable.
func (h *hash) demotable() bool {
	return h.enc == encHashtable && h.ft.hasResident()
}
