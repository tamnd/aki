package replay_test

import (
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/replay"
	"github.com/tamnd/aki/engine/obs1/store"
)

func newApplier(t *testing.T) (*replay.Applier, *store.Store) {
	t.Helper()
	st := store.New(16<<20, 1<<20)
	t.Cleanup(func() { _ = st.Close() })
	return replay.New(replay.Config{Store: func([]byte) *store.Store { return st }}), st
}

// frame encodes one op the way the owner's emission path would, failing
// the test on an encode refusal.
func frame(t *testing.T, seq uint64, key string, op obs1.Op) obs1.WALFrame {
	t.Helper()
	var k []byte
	if key != "" {
		k = []byte(key)
	}
	f, err := obs1.EncodeOp(0, seq, k, op)
	if err != nil {
		t.Fatalf("EncodeOp(%T): %v", op, err)
	}
	return f
}

func apply(t *testing.T, a *replay.Applier, group uint16, f obs1.WALFrame) {
	t.Helper()
	if err := a.Apply(group, f); err != nil {
		t.Fatalf("Apply(kind 0x%02x seq %d): %v", f.Kind, f.Seq, err)
	}
}

func wantString(t *testing.T, st *store.Store, key, want string) {
	t.Helper()
	v, ok := st.GetString([]byte(key), 0, nil)
	if !ok {
		t.Fatalf("key %q is absent, want %q", key, want)
	}
	if string(v) != want {
		t.Fatalf("key %q holds %q, want %q", key, v, want)
	}
}

func TestApplierStringPlane(t *testing.T) {
	a, st := newApplier(t)

	apply(t, a, 0, frame(t, 1, "alpha", obs1.StrSet{Value: []byte("one"), ExpiryMS: 5000}))
	apply(t, a, 0, frame(t, 2, "bravo", obs1.StrSet{Value: []byte("gone")}))
	apply(t, a, 0, frame(t, 3, "bravo", obs1.KeyDel{}))
	apply(t, a, 0, frame(t, 4, "charlie", obs1.KeyDel{}))
	apply(t, a, 0, frame(t, 5, "alpha", obs1.Expire{ExpiryMS: 9000}))
	apply(t, a, 0, frame(t, 6, "", obs1.Noop{Pad: []byte{0, 0}}))
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	wantString(t, st, "alpha", "one")
	if at := st.ExpireAt([]byte("alpha"), 0); at != 9000 {
		t.Fatalf("alpha deadline is %d, want 9000 after the expire frame", at)
	}
	if _, ok := st.GetString([]byte("bravo"), 0, nil); ok {
		t.Fatalf("bravo survived its keydel")
	}
	want := replay.Stats{Frames: 6, StrSets: 2, Dels: 1, DelMisses: 1, Expires: 1, Noops: 1}
	if got := a.Stats(); got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// TestApplierReplayNeverExpires proves the now-zero rule: a deadline
// already in the past when the node boots must not lazy-expire mid
// replay, or a later frame naming the key would read divergence where
// there is none.
func TestApplierReplayNeverExpires(t *testing.T) {
	a, st := newApplier(t)

	apply(t, a, 0, frame(t, 1, "stale", obs1.StrSet{Value: []byte("old"), ExpiryMS: 1}))
	apply(t, a, 0, frame(t, 2, "stale", obs1.Expire{ExpiryMS: 2}))
	wantString(t, st, "stale", "old")
	if _, ok := st.GetString([]byte("stale"), 1_000_000, nil); ok {
		t.Fatalf("stale should lazy-expire at serve time, the deadline is long past")
	}
}

func TestApplierTxnRunIsAtomic(t *testing.T) {
	a, st := newApplier(t)

	apply(t, a, 2, frame(t, 1, "", obs1.Txn{Begin: true}))
	apply(t, a, 2, frame(t, 2, "x", obs1.StrSet{Value: []byte("vx")}))
	if _, ok := st.GetString([]byte("x"), 0, nil); ok {
		t.Fatalf("x landed before the run closed")
	}
	apply(t, a, 2, frame(t, 3, "y", obs1.StrSet{Value: []byte("vy")}))
	apply(t, a, 2, frame(t, 4, "", obs1.Txn{End: true}))
	wantString(t, st, "x", "vx")
	wantString(t, st, "y", "vy")
	if got := a.Stats(); got.TxnRuns != 1 || got.StrSets != 2 {
		t.Fatalf("stats %+v, want one closed run of two strsets", got)
	}

	apply(t, a, 2, frame(t, 5, "", obs1.Txn{Begin: true}))
	apply(t, a, 2, frame(t, 6, "z", obs1.StrSet{Value: []byte("vz")}))
	err := a.Finish()
	if err == nil || !strings.Contains(err.Error(), "open txn run") {
		t.Fatalf("Finish over a dangling run: %v", err)
	}
	if _, ok := st.GetString([]byte("z"), 0, nil); ok {
		t.Fatalf("z landed out of a run that never closed")
	}
}

func TestApplierTxnMarkerDiscipline(t *testing.T) {
	a, _ := newApplier(t)
	if err := a.Apply(0, frame(t, 1, "", obs1.Txn{End: true})); err == nil {
		t.Fatalf("an end without a begin must refuse")
	}

	b, _ := newApplier(t)
	apply(t, b, 0, frame(t, 1, "", obs1.Txn{Begin: true}))
	if err := b.Apply(0, frame(t, 2, "", obs1.Txn{Begin: true})); err == nil {
		t.Fatalf("a nested begin must refuse")
	}
}

func TestApplierExpireOnAbsentKeyIsLoud(t *testing.T) {
	a, _ := newApplier(t)
	err := a.Apply(0, frame(t, 1, "ghost", obs1.Expire{ExpiryMS: 100}))
	if err == nil || !strings.Contains(err.Error(), "diverged") {
		t.Fatalf("expire on an absent key: %v", err)
	}
}

func TestApplierRefusesCollectionKinds(t *testing.T) {
	a, _ := newApplier(t)
	ops := []obs1.Op{
		obs1.CollNew{Type: obs1.CollSet},
		obs1.CollDelta{Sub: obs1.SAdd{Members: [][]byte{[]byte("m")}}},
		obs1.CollDrop{},
	}
	for _, op := range ops {
		err := a.Apply(0, frame(t, 1, "c", op))
		if err == nil || !strings.Contains(err.Error(), "collection replay is not wired yet") {
			t.Fatalf("Apply(%T): %v", op, err)
		}
	}
}
