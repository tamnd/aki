package set

import "github.com/tamnd/aki/engine/f3/shard"

// The F13 hot-key draw escalation (spec 2064/f3/11 section 5.4). The adversarial
// SPOP row hammers one very large set from every client, and F1 caps that key at
// one core. Section 5.1's budget says one owner clears the 2.1M bar with margin,
// so escalation is default off and pre-registered as headroom; the M1 gate read
// 1.50M ops/s inside the 1.06M-2.1M band, which is exactly the pre-registered
// case that makes F13 load-bearing (results/f3/m1-gate.md, PRED-F3-M1-SPOP), so
// this slice wires the escalation the doc named.
//
// The mechanism, per section 5.4: the set is already partitioned at this size,
// so escalation groups its P sub-tables into k draw-groups, each group a
// contiguous span of partitions with its own subtotal live count. A draw is two
// levels. First the scatter layer picks a group weighted by its share of the
// total count. Then the group draws within its own partitions by the same
// exactly-uniform weighted prefix-sum the flat band uses (section 4.3). Composed,
// the two levels are identical in distribution to the flat single-owner weighted
// draw for every draw position, so uniformity stays exact (F15) and the
// chi-squared gate is unchanged; the split exists so the execution model can hand
// each group's partition span to a different owning worker, which is the F13
// fan-out the saturated single owner needs.
//
// Off by default and one-way. A set escalates only when the execution model sees
// its owner saturated on a draw-heavy mix (the section-5.4 trigger, which lives
// in the execution model), and it never de-escalates, matching the one-way band
// policy (F4): an SPOP drain that thins the set keeps the escalated layout, so
// there is no downward conversion on the draw path. The scatter weights are the
// per-group subtotals maintained inline on add and rem, so on a single owner the
// weighting is exact per draw; the doc permits the cross-worker snapshot to be
// epoch-stale by at most one batch, which is the same staleness Redis exhibits
// between sampling and replying, and the execution model applies that relaxation
// when it reads a peer group's total across cores.

const (
	// escalateMinP is the partition floor below which draw escalation is refused.
	// Below it the flat weighted locate is already a handful of L1-resident counts,
	// and there are too few partitions to split k ways without a group holding a
	// single partition, where the scatter scan buys nothing over the flat scan it
	// wraps (lab 07, labs/f3/m1/07_draw_escalation). At or above it a k-way split
	// leaves every group a real span of partitions to draw within, which is the
	// unit the fan-out hands a worker. A partitioned set reaches P8 near 2M members
	// and P16 near 4M, so the hot-key SPOP row (a multi-million-member set) is
	// always above this floor by construction.
	escalateMinP = 8
)

// escalation splits a partitioned set's parts into k contiguous draw-groups for
// the F13 fan-out (section 5.4). span is the partitions per group, an exact
// divide of P since escalate only accepts a k that divides P, and totals is the
// per-group subtotal live count, the scatter weight, maintained inline on add and
// rem so it never scans the partitions to reweight. len(totals) is k.
type escalation struct {
	span   int
	totals []uint64
}

// newEscalation builds the per-group subtotals from the live per-partition counts
// in one pass, the section-5.4 scatter table. k is the group count and span is
// P/k; group g owns partitions [g*span, (g+1)*span).
func newEscalation(counts []uint64, span, k int) *escalation {
	e := &escalation{span: span, totals: make([]uint64, k)}
	for g := 0; g < k; g++ {
		var t uint64
		for i := g * span; i < (g+1)*span; i++ {
			t += counts[i]
		}
		e.totals[g] = t
	}
	return e
}

// pickGroup maps a flat draw position r in [0,total) to its draw-group and the
// group-local position, the F13 scatter layer (section 5.4): r lands in group g
// with probability totals[g]/total, and rLocal is then uniform in [0,totals[g]).
// The scan is the same branchless prefix-sum as locate (partition.go), over the k
// group subtotals instead of the P partition counts, so it costs a handful of
// L1-resident adds. It is the fan-out seam: the execution model routes r to the
// worker owning group g, and that worker draws rLocal within its own span with no
// cross-core read on the hot path.
func (e *escalation) pickGroup(r uint64) (group int, rLocal uint64) {
	var end, start uint64
	g := 0
	for i := 0; i < len(e.totals); i++ {
		end += e.totals[i]
		// mask is all-ones when r >= end (group i lies entirely before r) and zero
		// otherwise, the same signed-shift trick locate uses; end runs the full
		// prefix sum while start accumulates only the groups before r, so their
		// difference gives the group-local position. Totals sum well under 2^63, so
		// the subtraction never overflows.
		mask := ^uint64(int64(r-end) >> 63)
		g += int(mask & 1)
		start += e.totals[i] & mask
	}
	return g, r - start
}

// escalate splits the partitions into k draw-groups, the F13 engagement (section
// 5.4). It is one-way (F4): an already-escalated set keeps its current split, so
// a repeated trigger is a no-op and the draw path never de-escalates. It refuses
// unless P is at least escalateMinP and k divides P with k at least 2, and
// reports whether it engaged.
func (pt *partitioned) escalate(k int) bool {
	if pt.esc != nil {
		return false
	}
	P := len(pt.parts)
	if P < escalateMinP || k < 2 || P%k != 0 {
		return false
	}
	pt.esc = newEscalation(pt.counts, P/k, k)
	return true
}

// escalated reports whether the set has engaged F13 draw escalation.
func (pt *partitioned) escalated() bool { return pt.esc != nil }

// locateEscalated resolves a flat draw index to its partition and in-partition
// slot through the two-level F13 draw: the scatter layer picks the group, then
// the group's own span is walked by the same weighted prefix-sum locate uses
// (section 4.3). Composed over the same flat r it is identical to locate for
// every r, so the mapping stays an exact bijection over [0,total) and uniformity
// is unchanged; the two levels exist so the group step can run on a different
// worker than the coordinator once the execution model fans the draw out.
func (pt *partitioned) locateEscalated(r uint64) (part, local int) {
	group, rLocal := pt.esc.pickGroup(r)
	base := group * pt.esc.span
	var end, start uint64
	p := base
	for i := base; i < base+pt.esc.span; i++ {
		end += pt.counts[i]
		mask := ^uint64(int64(rLocal-end) >> 63)
		p += int(mask & 1)
		start += pt.counts[i] & mask
	}
	return p, int(rLocal - start)
}

// escalateDraws engages F13 draw escalation on a set, the seam the execution
// model drives when its saturation trigger fires on a draw-heavy hot key
// (section 5.4). It is a no-op on any band but the partitioned one and on an
// already-escalated set, and reports whether it engaged.
func (s *set) escalateDraws(workers int) bool {
	if s.enc != encPartitioned {
		return false
	}
	return s.part.escalate(workers)
}

// EscalateHotDraws is the execution model's entry to the F13 escalation: when the
// reactor sees one key's owner saturated on a draw-heavy mix (the section-5.4
// trigger, which the execution model owns because only it observes queue depth),
// it calls this with the worker count it can donate. It engages the partitioned
// set's k-way draw split and reports whether escalation took, so the caller can
// record the F13 usage the gate row asks for. It never de-escalates; a key that
// cools keeps its layout until it is dropped (F4).
func EscalateHotDraws(cx *shard.Ctx, key []byte, workers int) bool {
	g := registry(cx)
	s, wrong := g.lookup(cx, key)
	if wrong || s == nil {
		return false
	}
	return s.escalateDraws(workers)
}
