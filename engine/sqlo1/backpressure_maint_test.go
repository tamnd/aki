package sqlo1

// Maintenance rung tests over a fake Maintainer: the WAL rung's due
// and force lines, the free-extent rung's foreground promotion, the
// shed verdict with its one repair pass, and the ladder property that
// responses are monotone in the signals.

import (
	"context"
	"errors"
	"testing"
)

// fakeMaint is a Maintainer with dial-a-pressure signals. The hooks
// let a test model a store whose maintenance actually helps.
type fakeMaint struct {
	p         Pressure
	ckpts     int
	compacts  int
	onCkpt    func(*fakeMaint)
	onCompact func(*fakeMaint)
}

func (f *fakeMaint) Pressure() Pressure { return f.p }

func (f *fakeMaint) Checkpoint() error {
	f.ckpts++
	if f.onCkpt != nil {
		f.onCkpt(f)
	}
	return nil
}

func (f *fakeMaint) CompactOnce(ctx context.Context) (bool, error) {
	f.compacts++
	if f.onCompact != nil {
		f.onCompact(f)
	}
	return true, nil
}

func newMaintLadder(f *fakeMaint) *ladder {
	ht := NewHotTable(64)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	return newLadder(ht, d, newEvictor(ht, 31), f)
}

func TestWalRungLines(t *testing.T) {
	ctx := context.Background()

	// Under the due line: neither path checkpoints.
	f := &fakeMaint{p: Pressure{Wal: 0.5}}
	l := newMaintLadder(f)
	if _, err := l.step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if f.ckpts != 0 || f.compacts != 0 {
		t.Fatalf("wal 0.5 spent %d checkpoints and %d compactions", f.ckpts, f.compacts)
	}

	// Due but not overdue: the tick checkpoints, the command path
	// does not.
	f = &fakeMaint{p: Pressure{Wal: 2}}
	l = newMaintLadder(f)
	if _, err := l.step(ctx); err != nil {
		t.Fatal(err)
	}
	if f.ckpts != 0 {
		t.Fatal("wal 2 checkpointed on the command path under the force line")
	}
	if err := l.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if f.ckpts != 1 {
		t.Fatalf("wal 2 tick took %d checkpoints, want 1", f.ckpts)
	}

	// Overdue: the command path pays.
	f = &fakeMaint{p: Pressure{Wal: 5}}
	l = newMaintLadder(f)
	if _, err := l.step(ctx); err != nil {
		t.Fatal(err)
	}
	if f.ckpts != 1 {
		t.Fatalf("wal 5 step took %d checkpoints, want 1", f.ckpts)
	}
}

func TestExtentRungForeground(t *testing.T) {
	ctx := context.Background()

	// Any positive extent pressure promotes compaction to the
	// foreground on both paths.
	f := &fakeMaint{p: Pressure{Extent: 0.4}}
	l := newMaintLadder(f)
	if _, err := l.step(ctx); err != nil {
		t.Fatal(err)
	}
	if f.compacts != 1 {
		t.Fatalf("extent 0.4 step ran %d compactions, want 1", f.compacts)
	}
	if err := l.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if f.compacts != 2 {
		t.Fatalf("extent 0.4 tick ran %d compactions total, want 2", f.compacts)
	}
	if f.ckpts != 0 {
		t.Fatal("extent pressure alone must not checkpoint")
	}
}

func TestShedRepairAndVerdict(t *testing.T) {
	ctx := context.Background()

	// A store whose repair pass frees headroom: shed clears and the
	// write goes through, with both verbs spent in order.
	f := &fakeMaint{p: Pressure{Shed: true, Extent: 1.5}}
	f.onCkpt = func(f *fakeMaint) { f.p = Pressure{} }
	l := newMaintLadder(f)
	shed, err := l.shed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if shed {
		t.Fatal("shed verdict stuck after a repair pass that freed headroom")
	}
	if f.compacts != 1 || f.ckpts != 1 {
		t.Fatalf("repair pass spent %d compactions and %d checkpoints, want 1 and 1", f.compacts, f.ckpts)
	}

	// A store still at the floor after repair sheds for real, and a
	// healthy store never enters the path.
	f = &fakeMaint{p: Pressure{Shed: true, Extent: 2}}
	l = newMaintLadder(f)
	if shed, err := l.shed(ctx); err != nil || !shed {
		t.Fatalf("sticky floor: shed=%v err=%v, want true", shed, err)
	}
	f = &fakeMaint{}
	l = newMaintLadder(f)
	if shed, err := l.shed(ctx); err != nil || shed {
		t.Fatalf("healthy store: shed=%v err=%v, want false", shed, err)
	}
	if f.compacts != 0 || f.ckpts != 0 {
		t.Fatal("healthy store paid maintenance from the shed check")
	}
}

// TestMaintenanceMonotone is the ladder property test: pointwise
// higher pressure never provokes a smaller response set. Responses
// are checkpoint-in-step, checkpoint-in-tick, compaction, and the
// shed verdict.
func TestMaintenanceMonotone(t *testing.T) {
	ctx := context.Background()
	type resp struct {
		ckptStep, ckptTick, compact, shed bool
	}
	respond := func(p Pressure) resp {
		var r resp
		f := &fakeMaint{p: p}
		l := newMaintLadder(f)
		if _, err := l.step(ctx); err != nil {
			t.Fatal(err)
		}
		r.ckptStep = f.ckpts > 0
		r.compact = f.compacts > 0
		f.ckpts, f.compacts = 0, 0
		if err := l.tick(ctx); err != nil {
			t.Fatal(err)
		}
		r.ckptTick = f.ckpts > 0
		r.compact = r.compact || f.compacts > 0
		shed, err := l.shed(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r.shed = shed
		return r
	}
	leq := func(a, b Pressure) bool {
		return a.Wal <= b.Wal && a.Extent <= b.Extent && (!a.Shed || b.Shed)
	}
	implies := func(a, b bool) bool { return !a || b }

	var grid []Pressure
	for _, w := range []float64{0, 0.5, 1, 4, 5} {
		for _, e := range []float64{0, 0.5, 1, 2} {
			for _, sh := range []bool{false, true} {
				grid = append(grid, Pressure{Wal: w, Extent: e, Shed: sh})
			}
		}
	}
	for _, p := range grid {
		rp := respond(p)
		for _, q := range grid {
			if !leq(p, q) {
				continue
			}
			rq := respond(q)
			ok := implies(rp.ckptStep, rq.ckptStep) &&
				implies(rp.ckptTick, rq.ckptTick) &&
				implies(rp.compact, rq.compact) &&
				implies(rp.shed, rq.shed)
			if !ok {
				t.Fatalf("response shrank as pressure rose: %+v -> %+v but %+v -> %+v", p, rp, q, rq)
			}
		}
	}
}

// memMaint is a MemStore that also feels pressure, for driving the
// Tiered write door without a real file.
type memMaint struct {
	*MemStore
	fakeMaint
}

func TestTieredShedDoor(t *testing.T) {
	ctx := context.Background()
	ms := &memMaint{MemStore: NewMemStore()}
	tr := NewTiered(ms, TieredConfig{Budget: Budget{Entries: 64, Arenas: 64 << 20}, Seed: 5})
	if err := tr.Set(ctx, []byte("pre"), []byte("v"), TagString); err != nil {
		t.Fatal(err)
	}

	// A sticky floor bounces writes with ErrShed; reads and deletes
	// keep working, which is the honest-failure contract.
	ms.p = Pressure{Shed: true, Extent: 2}
	if err := tr.Set(ctx, []byte("k"), []byte("v"), TagString); !errors.Is(err, ErrShed) {
		t.Fatalf("Set under shed returned %v, want ErrShed", err)
	}
	if v, ok, err := tr.Get(ctx, []byte("pre")); err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get under shed: %q %v %v", v, ok, err)
	}
	if gone, err := tr.Del(ctx, []byte("pre")); err != nil || !gone {
		t.Fatalf("Del under shed: %v %v, deletes must stay exempt", gone, err)
	}

	// Recovery is automatic: the moment a repair pass frees headroom,
	// the same Set goes through.
	ms.onCkpt = func(f *fakeMaint) { f.p = Pressure{} }
	if err := tr.Set(ctx, []byte("k"), []byte("v"), TagString); err != nil {
		t.Fatalf("Set after repair freed headroom: %v", err)
	}
	if ms.compacts == 0 || ms.ckpts == 0 {
		t.Fatalf("repair pass spent %d compactions and %d checkpoints", ms.compacts, ms.ckpts)
	}
}
