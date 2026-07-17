package shard

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// TestRuntimeFoldTapFeedsDrains pins the runtime pass-through: a tap set
// through the Runtime hears every drain the worker path stages, on the
// owner goroutine, with bytes that walk as staged frames, and the drains
// themselves complete unchanged behind it.
func TestRuntimeFoldTapFeedsDrains(t *testing.T) {
	const cap = 1 << 20
	s := drainStore(t, cap)
	w := newWorker(0, s)
	r := &Runtime{workers: []*worker{w}}
	w.rt = r

	taps, frames := 0, 0
	r.SetFoldTap(func(buf []byte) {
		if len(buf) == 0 {
			t.Error("tap fired with an empty buffer")
		}
		taps++
		if err := store.WalkStagedFrames(buf, func(store.FoldFrame) error {
			frames++
			return nil
		}); err != nil {
			t.Errorf("tapped buffer does not walk: %v", err)
		}
	})

	const n = 40000
	for i := range n {
		k := fmt.Appendf(nil, "k:%07d", i)
		v := fmt.Appendf(nil, "v-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}
	for pass := 0; pass < 32 && s.NeedsColdDrain(); pass++ {
		w.drainCold()
		w.advanceIntents()
	}
	w.io.stop()
	for i := 0; i < 64 && w.io.pool.out > 0; i++ {
		if w.advanceIntents() == 0 {
			break
		}
	}

	if taps == 0 || frames == 0 {
		t.Fatalf("tap heard %d drains, %d frames, want both nonzero", taps, frames)
	}
	if s.Cold().Records == 0 {
		t.Fatal("no records migrated cold behind the tap")
	}
	if uint64(frames) < s.Cold().Records {
		t.Fatalf("tap walked %d frames but %d records went cold", frames, s.Cold().Records)
	}
}
