package sqlo1b

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
)

var errBoom = errors.New("injected crash")

type fakeSrc struct {
	calls   []string
	ts      []uint64
	roots   Roots
	onDrain func()
}

func (s *fakeSrc) step(name string, t uint64) {
	s.calls = append(s.calls, name)
	s.ts = append(s.ts, t)
}

func (s *fakeSrc) Drain(t uint64) error {
	s.step("drain", t)
	if s.onDrain != nil {
		s.onDrain()
	}
	return nil
}

func (s *fakeSrc) FlushIndex(t uint64) error {
	s.step("flush", t)
	return nil
}

func (s *fakeSrc) Snapshot(t uint64) (Roots, error) {
	s.step("snapshot", t)
	return s.roots, nil
}

type ckptRig struct {
	f    *os.File
	wal  *sqlo1.WAL
	grid *Grid
	sb   *Superblock
	quar uint64
	src  *fakeSrc
}

// newCkptRig builds a shard mid-life: superblock at seq 1 on a real
// file, three data frames in a real sidecar, and one quarantined
// extent tagged with the seq the next checkpoint will commit.
func newCkptRig(t *testing.T) *ckptRig {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "b.aki"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}
	w, err := sqlo1.OpenWAL(filepath.Join(dir, "b.aki-wal"), 42, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	for i := range 3 {
		if _, err := w.Append(0, sqlo1.WALOpPut, 0, fmt.Appendf(nil, "put %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	g := NewGrid(8)
	ext, err := g.Allocate(KindVlog, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Seal(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if err := g.Free(ext, sb.Seq+1); err != nil {
		t.Fatal(err)
	}

	return &ckptRig{f: f, wal: w, grid: g, sb: sb, quar: ext, src: &fakeSrc{
		roots: Roots{
			Dir:          FullPtr{Pos: 101, Sum: 201},
			Allocmap:     FullPtr{Pos: 102, Sum: 202},
			Dict:         FullPtr{Pos: 103, Sum: 203},
			Stats:        FullPtr{Pos: 104, Sum: 204},
			RecordCount:  7,
			GarbageBytes: 9,
			HighWater:    11,
		},
	}}
}

// fold replays the rig's WAL from a trim barrier into a FormatState.
func (r *ckptRig) fold(t *testing.T, trim uint64) (FormatState, int) {
	t.Helper()
	r.wal.SetTrim(trim)
	var st FormatState
	frames := 0
	err := r.wal.Replay(func(fr sqlo1.WALFrame) error {
		frames++
		return st.Apply(fr.Seq, fr.Op, fr.Payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	return st, frames
}

func TestCheckpointRun(t *testing.T) {
	r := newCkptRig(t)
	c := &Checkpointer{WAL: r.wal, File: r.f, Grid: r.grid}
	next, err := c.Run(r.sb, r.src)
	if err != nil {
		t.Fatal(err)
	}

	if got := fmt.Sprint(r.src.calls); got != "[drain flush snapshot]" {
		t.Fatalf("step order %s", got)
	}
	for i, ts := range r.src.ts {
		if ts != 3 {
			t.Fatalf("step %d got target %d, want the frozen 3", i, ts)
		}
	}
	if next.Seq != 2 || next.WALTrimSeq != 3 {
		t.Fatalf("next seq %d trim %d", next.Seq, next.WALTrimSeq)
	}
	if next.DirRoot != r.src.roots.Dir || next.StatsRoot != r.src.roots.Stats ||
		next.RecordCount != 7 || next.GarbageBytes != 9 || next.HighWater != 11 {
		t.Fatal("roots did not land in the superblock")
	}

	got, slot, err := ReadSuperblock(r.f)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 2 || slot != slotFor(t, 2) {
		t.Fatalf("disk shows seq %d slot %d", got.Seq, slot)
	}
	if got.AllocmapRoot != r.src.roots.Allocmap {
		t.Fatal("committed superblock missing the snapshot roots")
	}

	// The trim barrier hides the checkpointed frames; only the CKPT
	// frame remains, and it names the committed superblock.
	st, frames := r.fold(t, next.WALTrimSeq)
	if frames != 1 || st.CkptSuper != 2 {
		t.Fatalf("replay past trim saw %d frames, ckpt super %d", frames, st.CkptSuper)
	}
	if r.grid.State(r.quar) != StateFree {
		t.Fatalf("quarantined extent is %s after a durable seq 2", r.grid.State(r.quar))
	}
}

// slotFor mirrors the seq parity rule without exporting it.
func slotFor(t *testing.T, seq uint64) int {
	t.Helper()
	if seq%2 == 0 {
		return 0
	}
	return 1
}

// TestCheckpointFrozenTarget pins step 1: the WAL growing during the
// drain must not move the target the later steps see.
func TestCheckpointFrozenTarget(t *testing.T) {
	r := newCkptRig(t)
	r.src.onDrain = func() {
		for range 5 {
			if _, err := r.wal.Append(0, sqlo1.WALOpPut, 0, []byte("late")); err != nil {
				t.Fatal(err)
			}
		}
	}
	c := &Checkpointer{WAL: r.wal, File: r.f, Grid: r.grid}
	next, err := c.Run(r.sb, r.src)
	if err != nil {
		t.Fatal(err)
	}
	for i, ts := range r.src.ts {
		if ts != 3 {
			t.Fatalf("step %d saw target %d after concurrent appends, want 3", i, ts)
		}
	}
	if next.WALTrimSeq != 3 {
		t.Fatalf("trim %d, want the frozen 3", next.WALTrimSeq)
	}
	// The late frames survive the barrier for the next replay.
	st, frames := r.fold(t, next.WALTrimSeq)
	if frames != 6 || st.CkptSuper != 2 {
		t.Fatalf("replay saw %d frames, ckpt %d; late writes must outlive the trim", frames, st.CkptSuper)
	}
}

// TestCheckpointCrashBoundaries walks a crash at every step
// boundary and asserts the disk state recovery would find: before
// step 5 the old superblock rules and nothing moved; after step 5
// the new superblock rules even though the CKPT frame never made it.
func TestCheckpointCrashBoundaries(t *testing.T) {
	for step := 1; step <= 6; step++ {
		t.Run(fmt.Sprintf("crash-after-step-%d", step), func(t *testing.T) {
			r := newCkptRig(t)
			c := &Checkpointer{WAL: r.wal, File: r.f, Grid: r.grid,
				crash: func(s int) error {
					if s == step {
						return errBoom
					}
					return nil
				}}
			next, err := c.Run(r.sb, r.src)
			if step == 6 {
				// The protocol finished; only the return was lost.
				if !errors.Is(err, errBoom) || next == nil {
					t.Fatalf("step 6 crash: next %v err %v", next, err)
				}
			} else if err == nil {
				t.Fatal("crash did not surface")
			}

			got, _, rerr := ReadSuperblock(r.f)
			if rerr != nil {
				t.Fatal(rerr)
			}
			switch {
			case step <= 4:
				if got.Seq != 1 {
					t.Fatalf("superblock moved to seq %d before the commit point", got.Seq)
				}
				st, frames := r.fold(t, got.WALTrimSeq)
				if frames != 3 || st.CkptSuper != 0 {
					t.Fatalf("pre-commit crash: %d frames, ckpt %d", frames, st.CkptSuper)
				}
				if r.grid.State(r.quar) != StateQuarantined {
					t.Fatal("quarantine released before the commit point")
				}
			case step == 5:
				// Superblock durable, CKPT frame lost: recovery opens
				// through seq 2 and replays past its trim barrier.
				if got.Seq != 2 || got.WALTrimSeq != 3 {
					t.Fatalf("post-commit crash: seq %d trim %d", got.Seq, got.WALTrimSeq)
				}
				st, frames := r.fold(t, got.WALTrimSeq)
				if frames != 0 || st.CkptSuper != 0 {
					t.Fatalf("post-commit crash: %d frames, ckpt %d", frames, st.CkptSuper)
				}
				if r.grid.State(r.quar) != StateQuarantined {
					t.Fatal("quarantine released without step 6")
				}
			default: // step 6
				if got.Seq != 2 {
					t.Fatalf("finished checkpoint shows seq %d", got.Seq)
				}
				st, frames := r.fold(t, got.WALTrimSeq)
				if frames != 1 || st.CkptSuper != 2 {
					t.Fatalf("finished checkpoint: %d frames, ckpt %d", frames, st.CkptSuper)
				}
				if r.grid.State(r.quar) != StateFree {
					t.Fatal("quarantine not released by the finished checkpoint")
				}
			}
		})
	}
}

func TestCheckpointSourceErrors(t *testing.T) {
	r := newCkptRig(t)
	c := &Checkpointer{WAL: r.wal, File: r.f, Grid: r.grid,
		crash: func(int) error { return nil }}
	src := &failSrc{failOn: "drain"}
	for _, name := range []string{"drain", "flush", "snapshot"} {
		src.failOn = name
		if _, err := c.Run(r.sb, src); !errors.Is(err, errBoom) {
			t.Fatalf("%s failure: %v", name, err)
		}
		got, _, err := ReadSuperblock(r.f)
		if err != nil || got.Seq != 1 {
			t.Fatalf("%s failure moved the superblock: seq %d err %v", name, got.Seq, err)
		}
	}
}

type failSrc struct{ failOn string }

func (s *failSrc) Drain(t uint64) error {
	if s.failOn == "drain" {
		return errBoom
	}
	return nil
}

func (s *failSrc) FlushIndex(t uint64) error {
	if s.failOn == "flush" {
		return errBoom
	}
	return nil
}

func (s *failSrc) Snapshot(t uint64) (Roots, error) {
	if s.failOn == "snapshot" {
		return Roots{}, errBoom
	}
	return Roots{}, nil
}

func TestCheckpointPolicy(t *testing.T) {
	p := DefaultCheckpointPolicy()
	if p.Due(0, 0) {
		t.Fatal("due with nothing written and no time passed")
	}
	if !p.Due(256<<20, 0) {
		t.Fatal("not due at the byte cadence")
	}
	if !p.Due(0, time.Minute) {
		t.Fatal("not due at the time cadence")
	}
	if p.Due(256<<20-1, 59*time.Second) {
		t.Fatal("due just under both cadences")
	}
}
