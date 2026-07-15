package sqlo1

import "context"

// Backpressure ladder, doc 04 section 13: three pressure signals with
// continuous responses instead of cliff edges. Only the dirty rung is
// live in this slice; the WAL rung arrives with the WAL milestone's
// checkpoint cadence and the free-extent rung with the extent
// allocator, so their signals read zero here and what this slice pins
// is the ladder's shape and the dirty rung's behavior.
//
// The dirty rung: pressure is dirty bytes as a multiple of the drain
// threshold. Under 1 the drain scheduler's own trigger is enough; from
// 1 the owner spends one drain quantum between command batches; from 2
// the quanta turn mandatory and step drains until pressure is back
// under the force line, which smoothly trades peak write latency for
// bounded memory (the hot tier cap is hard, R5).

const (
	// dirtyPressureDrain is where drain quanta start riding command
	// batches; dirtyPressureForce is where they become mandatory.
	dirtyPressureDrain = 1.0
	dirtyPressureForce = 2.0
)

type ladder struct {
	ht *HotTable
	d  *drainer
	e  *evictor
}

func newLadder(ht *HotTable, d *drainer, e *evictor) *ladder {
	return &ladder{ht: ht, d: d, e: e}
}

// dirtyPressure is the dirty rung's signal.
func (l *ladder) dirtyPressure() float64 {
	return float64(l.ht.dirtyBytes) / float64(l.d.threshold)
}

// walPressure and extentPressure are the other rungs' signals; they
// read zero until the WAL slice gives checkpoint lag something to
// measure and the extent allocator reports its free reserve.
func (l *ladder) walPressure() float64    { return 0 }
func (l *ladder) extentPressure() float64 { return 0 }

// step runs between command batches and returns the drain quanta it
// spent: none under the drain line, one voluntary quantum from there,
// and mandatory quanta from the force line down to it, so a write
// burst hotter than the drain can absorb stalls the burst rather than
// growing dirty RAM without bound.
func (l *ladder) step(ctx context.Context) (int, error) {
	if l.dirtyPressure() < dirtyPressureDrain {
		return 0, nil
	}
	quanta := 0
	for {
		n, err := l.d.drain(ctx)
		if err != nil {
			return quanta, err
		}
		if n > 0 {
			quanta++
		}
		if n == 0 || l.dirtyPressure() < dirtyPressureForce {
			return quanta, nil
		}
	}
}

// makeRoom frees at least need bytes of hot-tier payload for a refused
// write: clean residents evict first, and when eviction cannot supply
// because everything left is dirty, it forces a drain cycle to cool
// records and continues (the doc 04 section 8 tail). Freed under need
// means the tier genuinely has nothing left to give at this capacity,
// and the caller surfaces that as the table being full.
func (l *ladder) makeRoom(ctx context.Context, need int) (int, error) {
	freed := 0
	for freed < need {
		freed += l.e.evict(need - freed)
		if freed >= need {
			break
		}
		n, err := l.d.drain(ctx)
		if err != nil {
			return freed, err
		}
		if n == 0 {
			break
		}
	}
	return freed, nil
}
