package sqlo1

import "context"

// Backpressure ladder, doc 04 section 13: three pressure signals with
// continuous responses instead of cliff edges.
//
// The dirty rung: pressure is dirty bytes as a multiple of the drain
// threshold. Under 1 the drain scheduler's own trigger is enough; from
// 1 the owner spends one drain quantum between command batches; from 2
// the quanta turn mandatory and step drains until pressure is back
// under the force line, which smoothly trades peak write latency for
// bounded memory (the hot tier cap is hard, R5).
//
// The WAL rung: pressure is checkpoint lag over the byte cadence. At
// 1 a checkpoint is due and the timer tick takes it off the command
// path; at 4 it is overdue enough that step spends the command lane
// on it, trading one command's latency for a bounded replay tail.
//
// The free-extent rung: pressure is free headroom sliding from the
// reserve toward the hard minimum. Any positive reading promotes
// compaction from background to foreground, and at the hard minimum
// writes shed with ErrShed, which is the honest failure mode. Both
// rungs come from the store's Maintainer surface; a store without one
// (MemStore) reads zero, same as when these were stubs.

const (
	// dirtyPressureDrain is where drain quanta start riding command
	// batches; dirtyPressureForce is where they become mandatory.
	dirtyPressureDrain = 1.0
	dirtyPressureForce = 2.0
	// walPressureCkpt is where a checkpoint is due, taken by the
	// timer tick; walPressureForce is where it rides the command path.
	walPressureCkpt  = 1.0
	walPressureForce = 4.0
)

type ladder struct {
	ht *HotTable
	d  *drainer
	e  *evictor
	// mt is the store's maintenance surface, nil when the store
	// cannot feel disk-side pressure.
	mt Maintainer
}

func newLadder(ht *HotTable, d *drainer, e *evictor, mt Maintainer) *ladder {
	return &ladder{ht: ht, d: d, e: e, mt: mt}
}

// dirtyPressure is the dirty rung's signal.
func (l *ladder) dirtyPressure() float64 {
	return float64(l.ht.dirtyBytes) / float64(l.d.threshold)
}

// walPressure and extentPressure read the store's gauges; maintain
// takes one snapshot instead of calling these twice.
func (l *ladder) walPressure() float64 {
	if l.mt == nil {
		return 0
	}
	return l.mt.Pressure().Wal
}

func (l *ladder) extentPressure() float64 {
	if l.mt == nil {
		return 0
	}
	return l.mt.Pressure().Extent
}

// step runs between command batches and returns the drain quanta it
// spent: none under the drain line, one voluntary quantum from there,
// and mandatory quanta from the force line down to it, so a write
// burst hotter than the drain can absorb stalls the burst rather than
// growing dirty RAM without bound. The maintenance rungs run after
// the drain work, with the checkpoint held back to the force line so
// it stays off the command path until the lag is real.
func (l *ladder) step(ctx context.Context) (int, error) {
	quanta := 0
	if l.dirtyPressure() >= dirtyPressureDrain {
		for {
			n, err := l.d.drain(ctx)
			if err != nil {
				return quanta, err
			}
			if n > 0 {
				quanta++
			}
			if n == 0 || l.dirtyPressure() < dirtyPressureForce {
				break
			}
		}
	}
	return quanta, l.maintain(ctx, walPressureForce)
}

// tick is the timer-driven half of the maintenance rungs: about once
// a second it takes any due checkpoint and any foreground compaction,
// so steady-state maintenance never rides a command.
func (l *ladder) tick(ctx context.Context) error {
	return l.maintain(ctx, walPressureCkpt)
}

// maintain runs the WAL and free-extent rungs off one pressure
// snapshot; walDue is the checkpoint line the caller pays at.
// Compaction runs before the checkpoint deliberately: compacted
// extents quarantine, and the checkpoint is what releases them to
// free, so this order is what turns garbage into headroom within one
// call when the store is shedding.
func (l *ladder) maintain(ctx context.Context, walDue float64) error {
	if l.mt == nil {
		return nil
	}
	p := l.mt.Pressure()
	if p.Extent > 0 || p.Shed {
		if _, err := l.mt.CompactOnce(ctx); err != nil {
			return err
		}
	}
	if p.Wal >= walDue || p.Shed {
		if err := l.mt.Checkpoint(); err != nil {
			return err
		}
	}
	return nil
}

// shed reports whether a write must bounce with ErrShed. A shedding
// store gets one repair pass first, compaction plus checkpoint, and
// only a store still at the floor after that says yes; recovery is
// automatic the same way, the moment a pass frees headroom.
func (l *ladder) shed(ctx context.Context) (bool, error) {
	if l.mt == nil || !l.mt.Pressure().Shed {
		return false, nil
	}
	if err := l.maintain(ctx, walPressureCkpt); err != nil {
		return true, err
	}
	return l.mt.Pressure().Shed, nil
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
