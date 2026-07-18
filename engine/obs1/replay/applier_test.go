package replay_test

import (
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/replay"
	"github.com/tamnd/aki/engine/obs1/set"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

func newApplier(t *testing.T) (*replay.Applier, *shard.Ctx) {
	t.Helper()
	st := store.New(16<<20, 1<<20)
	t.Cleanup(func() { _ = st.Close() })
	cx := &shard.Ctx{St: st}
	return replay.New(replay.Config{Ctx: func([]byte) *shard.Ctx { return cx }}), cx
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

// wantMembers checks the replayed set through the same registry the
// owner would serve from, via the type's exported card and membership
// probes over the test Ctx.
func wantMembers(t *testing.T, cx *shard.Ctx, key string, members ...string) {
	t.Helper()
	for _, m := range members {
		if !setHas(cx, key, m) {
			t.Fatalf("set %q is missing member %q", key, m)
		}
	}
}

// setHas probes membership by replaying a zero-effect srem: a member
// that removes and re-adds cleanly was present. Kept test-local so the
// production surface stays the three replay entry points.
func setHas(cx *shard.Ctx, key, member string) bool {
	if err := set.ReplayRem(cx, []byte(key), [][]byte{[]byte(member)}); err != nil {
		return false
	}
	if err := set.ReplayAdd(cx, []byte(key), [][]byte{[]byte(member)}, false); err != nil {
		panic(err)
	}
	return true
}

func TestApplierStringPlane(t *testing.T) {
	a, cx := newApplier(t)
	st := cx.St

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
	a, cx := newApplier(t)
	st := cx.St

	apply(t, a, 0, frame(t, 1, "stale", obs1.StrSet{Value: []byte("old"), ExpiryMS: 1}))
	apply(t, a, 0, frame(t, 2, "stale", obs1.Expire{ExpiryMS: 2}))
	wantString(t, st, "stale", "old")
	if _, ok := st.GetString([]byte("stale"), 1_000_000, nil); ok {
		t.Fatalf("stale should lazy-expire at serve time, the deadline is long past")
	}
}

func TestApplierTxnRunIsAtomic(t *testing.T) {
	a, cx := newApplier(t)
	st := cx.St

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

// TestApplierSetPlane replays the emitter shapes writelog.go frames for
// sets: collnew plus sadd on create, a bare sadd on an existing set,
// srem, and the srem plus colldrop pair when the last member leaves.
func TestApplierSetPlane(t *testing.T) {
	a, cx := newApplier(t)

	m := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	apply(t, a, 0, frame(t, 1, "s", obs1.CollNew{Type: obs1.CollSet}))
	apply(t, a, 0, frame(t, 2, "s", obs1.CollDelta{Sub: obs1.SAdd{Members: m("a", "b")}}))
	apply(t, a, 0, frame(t, 3, "s", obs1.CollDelta{Sub: obs1.SAdd{Members: m("c")}}))
	apply(t, a, 0, frame(t, 4, "s", obs1.CollDelta{Sub: obs1.SRem{Members: m("b")}}))
	wantMembers(t, cx, "s", "a", "c")
	if setHas(cx, "s", "b") {
		t.Fatalf("b survived its srem")
	}

	apply(t, a, 0, frame(t, 5, "s", obs1.CollDelta{Sub: obs1.SRem{Members: m("a", "c")}}))
	apply(t, a, 0, frame(t, 6, "s", obs1.CollDrop{}))
	if set.ReplayDrop(cx, []byte("s")) {
		t.Fatalf("s survived its colldrop")
	}
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	want := replay.Stats{Frames: 6, CollNews: 1, CollDrops: 1, SAdds: 2, SRems: 2}
	if got := a.Stats(); got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// TestApplierSetResetToEmpty replays the STORE form's shape over a live
// destination: a collnew on an existing set replaces it wholesale, the
// doc 04 reset-to-empty rule.
func TestApplierSetResetToEmpty(t *testing.T) {
	a, cx := newApplier(t)

	one := [][]byte{[]byte("old1"), []byte("old2")}
	apply(t, a, 0, frame(t, 1, "d", obs1.CollNew{Type: obs1.CollSet}))
	apply(t, a, 0, frame(t, 2, "d", obs1.CollDelta{Sub: obs1.SAdd{Members: one}}))
	apply(t, a, 0, frame(t, 3, "d", obs1.CollNew{Type: obs1.CollSet}))
	apply(t, a, 0, frame(t, 4, "d", obs1.CollDelta{Sub: obs1.SAdd{Members: [][]byte{[]byte("new")}}}))
	wantMembers(t, cx, "d", "new")
	if setHas(cx, "d", "old1") || setHas(cx, "d", "old2") {
		t.Fatalf("the old members survived the reset-to-empty collnew")
	}
}

// TestApplierKeyDelSpansKeyspaces proves keydel probes both the string
// store and the set registry: a DEL over a set key frames a keydel, and
// replay must actually remove the set.
func TestApplierKeyDelSpansKeyspaces(t *testing.T) {
	a, cx := newApplier(t)

	apply(t, a, 0, frame(t, 1, "k", obs1.CollNew{Type: obs1.CollSet}))
	apply(t, a, 0, frame(t, 2, "k", obs1.CollDelta{Sub: obs1.SAdd{Members: [][]byte{[]byte("m")}}}))
	apply(t, a, 0, frame(t, 3, "k", obs1.KeyDel{}))
	if set.ReplayDrop(cx, []byte("k")) {
		t.Fatalf("the set survived its keydel")
	}
	if got := a.Stats(); got.Dels != 1 || got.DelMisses != 0 {
		t.Fatalf("stats %+v, want the keydel counted as a hit", got)
	}
}

// TestApplierSetCorruptionIsLoud drives each divergence the set plane
// must refuse: a delta on a missing set, a member framed as added that
// is already present, one framed as removed that is absent, a colldrop
// on nothing, a collnew whose next frame is not its delta, and a
// dangling collnew at the end of the stream.
func TestApplierSetCorruptionIsLoud(t *testing.T) {
	one := [][]byte{[]byte("m")}
	cases := []struct {
		name string
		ops  []obs1.Op
		want string
	}{
		{"sadd on missing set", []obs1.Op{obs1.CollDelta{Sub: obs1.SAdd{Members: one}}}, "no set exists"},
		{"srem on missing set", []obs1.Op{obs1.CollDelta{Sub: obs1.SRem{Members: one}}}, "no set exists"},
		{"duplicate add", []obs1.Op{
			obs1.CollNew{Type: obs1.CollSet},
			obs1.CollDelta{Sub: obs1.SAdd{Members: one}},
			obs1.CollDelta{Sub: obs1.SAdd{Members: one}},
		}, "already in set"},
		{"absent remove", []obs1.Op{
			obs1.CollNew{Type: obs1.CollSet},
			obs1.CollDelta{Sub: obs1.SAdd{Members: one}},
			obs1.CollDelta{Sub: obs1.SRem{Members: [][]byte{[]byte("ghost")}}},
		}, "is not in set"},
		{"colldrop on nothing", []obs1.Op{obs1.CollDrop{}}, "no collection exists"},
		{"collnew without its delta", []obs1.Op{
			obs1.CollNew{Type: obs1.CollSet},
			obs1.StrSet{Value: []byte("v")},
		}, "not followed by its delta"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := newApplier(t)
			var err error
			for i, op := range tc.ops {
				if err = a.Apply(0, frame(t, uint64(i+1), "c", op)); err != nil {
					break
				}
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}

	a, _ := newApplier(t)
	apply(t, a, 0, frame(t, 1, "c", obs1.CollNew{Type: obs1.CollSet}))
	if err := a.Finish(); err == nil || !strings.Contains(err.Error(), "awaiting its delta") {
		t.Fatalf("Finish over a dangling collnew: %v", err)
	}
}

// TestApplierHashPlane replays the emitter shapes writelog.go frames for
// hashes: collnew plus hset on create, an overwriting hset, hdel, the
// hdel plus colldrop pair, and the hexpire deadline set, clear, and the
// restore that rides behind a TTL-preserving verb's hset.
func TestApplierHashPlane(t *testing.T) {
	a, cx := newApplier(t)

	fv := func(ss ...string) []obs1.FieldValue {
		out := make([]obs1.FieldValue, len(ss)/2)
		for i := range out {
			out[i] = obs1.FieldValue{Field: []byte(ss[2*i]), Value: []byte(ss[2*i+1])}
		}
		return out
	}
	fl := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	apply(t, a, 0, frame(t, 1, "h", obs1.CollNew{Type: obs1.CollHash}))
	apply(t, a, 0, frame(t, 2, "h", obs1.CollDelta{Sub: obs1.HSet{Pairs: fv("f1", "v1", "f2", "v2")}}))
	apply(t, a, 0, frame(t, 3, "h", obs1.CollDelta{Sub: obs1.HExpire{AtMs: 7_000_000_000_000, Fields: fl("f1")}}))
	apply(t, a, 0, frame(t, 4, "h", obs1.CollDelta{Sub: obs1.HSet{Pairs: fv("f1", "v1b")}}))
	apply(t, a, 0, frame(t, 5, "h", obs1.CollDelta{Sub: obs1.HExpire{AtMs: 8_000_000_000_000, Fields: fl("f1")}}))
	apply(t, a, 0, frame(t, 6, "h", obs1.CollDelta{Sub: obs1.HExpire{AtMs: 0, Fields: fl("f1")}}))
	apply(t, a, 0, frame(t, 7, "h", obs1.CollDelta{Sub: obs1.HDel{Fields: fl("f2")}}))
	if err := hash.ReplayHDel(cx, []byte("h"), fl("f1")); err != nil {
		t.Fatalf("f1 should survive the replay: %v", err)
	}
	if err := hash.ReplayHDel(cx, []byte("h"), fl("f2")); err == nil {
		t.Fatalf("f2 survived its hdel")
	}

	apply(t, a, 0, frame(t, 8, "hd", obs1.CollNew{Type: obs1.CollHash}))
	apply(t, a, 0, frame(t, 9, "hd", obs1.CollDelta{Sub: obs1.HSet{Pairs: fv("x", "1")}}))
	apply(t, a, 0, frame(t, 10, "hd", obs1.CollDelta{Sub: obs1.HDel{Fields: fl("x")}}))
	apply(t, a, 0, frame(t, 11, "hd", obs1.CollDrop{}))
	if hash.ReplayDrop(cx, []byte("hd")) {
		t.Fatalf("hd survived its colldrop")
	}
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got := a.Stats(); got.HSets != 3 || got.HDels != 2 || got.HExpires != 3 || got.CollNews != 2 || got.CollDrops != 1 {
		t.Fatalf("stats %+v, want the hash plane counts", got)
	}
}

// TestApplierHashCorruptionIsLoud drives the divergences the hash plane
// must refuse: deltas on a missing hash, an hdel of an absent field, and
// an hexpire naming a field that is not there.
func TestApplierHashCorruptionIsLoud(t *testing.T) {
	pairs := []obs1.FieldValue{{Field: []byte("f"), Value: []byte("v")}}
	one := [][]byte{[]byte("f")}
	cases := []struct {
		name string
		ops  []obs1.Op
		want string
	}{
		{"hset on missing hash", []obs1.Op{obs1.CollDelta{Sub: obs1.HSet{Pairs: pairs}}}, "no hash exists"},
		{"hdel on missing hash", []obs1.Op{obs1.CollDelta{Sub: obs1.HDel{Fields: one}}}, "no hash exists"},
		{"hexpire on missing hash", []obs1.Op{obs1.CollDelta{Sub: obs1.HExpire{AtMs: 5, Fields: one}}}, "no hash exists"},
		{"absent field hdel", []obs1.Op{
			obs1.CollNew{Type: obs1.CollHash},
			obs1.CollDelta{Sub: obs1.HSet{Pairs: pairs}},
			obs1.CollDelta{Sub: obs1.HDel{Fields: [][]byte{[]byte("ghost")}}},
		}, "is not in hash"},
		{"absent field hexpire", []obs1.Op{
			obs1.CollNew{Type: obs1.CollHash},
			obs1.CollDelta{Sub: obs1.HSet{Pairs: pairs}},
			obs1.CollDelta{Sub: obs1.HExpire{AtMs: 5, Fields: [][]byte{[]byte("ghost")}}},
		}, "is not in hash"},
		{"collnew hash consumed by srem", []obs1.Op{
			obs1.CollNew{Type: obs1.CollHash},
			obs1.CollDelta{Sub: obs1.SRem{Members: one}},
		}, "followed by sub-op"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := newApplier(t)
			var err error
			for i, op := range tc.ops {
				if err = a.Apply(0, frame(t, uint64(i+1), "c", op)); err != nil {
					break
				}
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// TestApplierRefusesUnwiredKinds keeps the loud refusal for the planes
// that have not landed: the other collection types and the consumer
// group vocabulary.
func TestApplierRefusesUnwiredKinds(t *testing.T) {
	a, _ := newApplier(t)
	if err := a.Apply(0, frame(t, 1, "z", obs1.CollNew{Type: obs1.CollZSet})); err == nil ||
		!strings.Contains(err.Error(), "not wired for replay yet") {
		t.Fatalf("collnew zset: %v", err)
	}
	entries := []obs1.ScoreMember{{Score: 1, Member: []byte("m")}}
	if err := a.Apply(0, frame(t, 2, "z", obs1.CollDelta{Sub: obs1.ZAdd{Entries: entries}})); err == nil ||
		!strings.Contains(err.Error(), "not wired for replay yet") {
		t.Fatalf("colldelta zadd: %v", err)
	}
	gd := obs1.GroupDelta{Sub: obs1.GNew{Group: []byte("g")}}
	if err := a.Apply(0, frame(t, 3, "st", gd)); err == nil || !strings.Contains(err.Error(), "not wired yet") {
		t.Fatalf("groupdelta: %v", err)
	}
}
