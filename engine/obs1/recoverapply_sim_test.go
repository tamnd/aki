package obs1_test

// The boot pipeline end to end (spec 2064/obs1 doc 04 sections 2 and 6):
// Recover plans the live WAL sections and the replay applier lands them
// in a real store, so the assertions here are on store contents, not on
// counted seqs. Folded state below the cursor is the manifest's business
// and stays out of the store until the keymap and cold read slices land.

import (
	"context"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/replay"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

func TestRecoverAppliesIntoStore(t *testing.T) {
	bucket := sim.New(sim.Config{})
	ctx := context.Background()
	const nodeA = uint64(0xA1)
	if err := obs1.CreateRoot(ctx, bucket, "p", false, obs1.Root{CreatedMS: 1, G: 4, D: 1}); err != nil {
		t.Fatal(err)
	}
	rr := newRecoverRig(t, bucket, nodeA)

	// Round 1: one frame per group, bravo under a deadline.
	if _, _, err := rr.wl.StrSet([]byte("alpha"), []byte("one"), 0, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rr.wl.StrSet([]byte("bravo"), []byte("b1"), 5000, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rr.wl.StrSet([]byte("charlie"), []byte("c1"), 0, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rr.wl.StrSet([]byte("delta"), []byte("d1"), 0, false); err != nil {
		t.Fatal(err)
	}
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)

	// Round 2: group 0 folds through the delete, leaving one live frame.
	if g, _, err := rr.wl.KeyDel([]byte("alpha")); err != nil || g != 0 {
		t.Fatalf("del: group %d err %v", g, err)
	}
	if _, _, err := rr.wl.StrSet([]byte("alpha"), []byte("two"), 0, false); err != nil {
		t.Fatal(err)
	}
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)
	rr.folder.Flush()
	rr.waitPublished(t, 1)
	rr.close(t)

	st := store.New(16<<20, 1<<20)
	t.Cleanup(func() { _ = st.Close() })
	ap := replay.New(replay.Config{Store: func([]byte) *store.Store { return st }})
	r, err := obs1.Recover(ctx, obs1.RecoverConfig{
		Store: bucket, Prefix: "p", DD: 0, Node: 0xB7, Incarnation: 1,
		Apply: ap.Apply,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Finish(); err != nil {
		t.Fatal(err)
	}

	// Group 0's pre-fold frames stayed with the manifest; the store holds
	// exactly the live tail.
	for key, want := range map[string]string{
		"alpha": "two", "bravo": "b1", "charlie": "c1", "delta": "d1",
	} {
		v, ok := st.GetString([]byte(key), 0, nil)
		if !ok || string(v) != want {
			t.Fatalf("key %q holds %q ok %v, want %q", key, v, ok, want)
		}
	}
	if at := st.ExpireAt([]byte("bravo"), 0); at != 5000 {
		t.Fatalf("bravo deadline %d, want the framed 5000", at)
	}
	stats := ap.Stats()
	if stats.Frames != r.Stats.FramesApplied {
		t.Fatalf("applier saw %d frames, recovery delivered %d", stats.Frames, r.Stats.FramesApplied)
	}
	want := replay.Stats{Frames: 4, StrSets: 4}
	if stats != want {
		t.Fatalf("stats %+v, want %+v: one live group 0 frame plus the three unfolded groups", stats, want)
	}
}
