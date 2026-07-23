package replay_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/list"
	"github.com/tamnd/aki/engine/obs1/replay"
	"github.com/tamnd/aki/engine/obs1/set"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/stream"
	"github.com/tamnd/aki/engine/obs1/zset"
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

// TestApplierZSetPlane replays the emitter shapes writelog.go frames for
// sorted sets: collnew plus zadd on create, a rescoring zadd on an
// existing sorted set, zrem, and the zrem plus colldrop pair when the
// last member leaves, with a STORE-shape reset over the live key.
func TestApplierZSetPlane(t *testing.T) {
	a, cx := newApplier(t)

	sm := func(pairs ...any) []obs1.ScoreMember {
		out := make([]obs1.ScoreMember, len(pairs)/2)
		for i := range out {
			out[i] = obs1.ScoreMember{Score: pairs[2*i].(float64), Member: []byte(pairs[2*i+1].(string))}
		}
		return out
	}
	m := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	apply(t, a, 0, frame(t, 1, "z", obs1.CollNew{Type: obs1.CollZSet}))
	apply(t, a, 0, frame(t, 2, "z", obs1.CollDelta{Sub: obs1.ZAdd{Entries: sm(1.0, "a", 2.0, "b")}}))
	apply(t, a, 0, frame(t, 3, "z", obs1.CollDelta{Sub: obs1.ZAdd{Entries: sm(5.5, "a", 3.0, "c")}}))
	apply(t, a, 0, frame(t, 4, "z", obs1.CollDelta{Sub: obs1.ZRem{Members: m("b")}}))
	if !zsetHolds(cx, "z", "a", 5.5) {
		t.Fatalf("a does not hold the rescored 5.5")
	}
	if !zsetHolds(cx, "z", "c", 3.0) {
		t.Fatalf("c does not hold 3.0")
	}
	if err := zset.ReplayZRem(cx, []byte("z"), m("b")); err == nil {
		t.Fatalf("b survived its zrem")
	}

	// STORE shape: a collnew over the live key resets it wholesale.
	apply(t, a, 0, frame(t, 5, "z", obs1.CollNew{Type: obs1.CollZSet}))
	apply(t, a, 0, frame(t, 6, "z", obs1.CollDelta{Sub: obs1.ZAdd{Entries: sm(9.0, "only")}}))
	if err := zset.ReplayZRem(cx, []byte("z"), m("a")); err == nil {
		t.Fatalf("a survived the reset-to-empty collnew")
	}

	apply(t, a, 0, frame(t, 7, "z", obs1.CollDelta{Sub: obs1.ZRem{Members: m("only")}}))
	apply(t, a, 0, frame(t, 8, "z", obs1.CollDrop{}))
	if zset.ReplayDrop(cx, []byte("z")) {
		t.Fatalf("z survived its colldrop")
	}

	apply(t, a, 0, frame(t, 9, "kd", obs1.CollNew{Type: obs1.CollZSet}))
	apply(t, a, 0, frame(t, 10, "kd", obs1.CollDelta{Sub: obs1.ZAdd{Entries: sm(1.0, "m")}}))
	apply(t, a, 0, frame(t, 11, "kd", obs1.KeyDel{}))
	if zset.ReplayDrop(cx, []byte("kd")) {
		t.Fatalf("the sorted set survived its keydel")
	}
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if got := a.Stats(); got.ZAdds != 4 || got.ZRems != 2 || got.CollNews != 3 || got.CollDrops != 1 || got.Dels != 1 {
		t.Fatalf("stats %+v, want the zset plane counts", got)
	}
}

// zsetHolds probes a replayed score by upserting the member at that
// score and reading the strictness refusal: a no-effect upsert is the
// one way ReplayZAdd errors on a live pair, so the refusal proves the
// member already holds the score. Kept test-local so the production
// surface stays the three entry points.
func zsetHolds(cx *shard.Ctx, key, member string, score float64) bool {
	err := zset.ReplayZAdd(cx, []byte(key), []float64{score}, [][]byte{[]byte(member)}, false)
	return err != nil && strings.Contains(err.Error(), "already holds its score")
}

// TestApplierZSetCorruptionIsLoud drives the divergences the zset plane
// must refuse: deltas on a missing sorted set, a pair framed as upserted
// that changes nothing, and a zrem of an absent member.
func TestApplierZSetCorruptionIsLoud(t *testing.T) {
	one := []obs1.ScoreMember{{Score: 1, Member: []byte("m")}}
	cases := []struct {
		name string
		ops  []obs1.Op
		want string
	}{
		{"zadd on missing zset", []obs1.Op{obs1.CollDelta{Sub: obs1.ZAdd{Entries: one}}}, "no sorted set exists"},
		{"zrem on missing zset", []obs1.Op{obs1.CollDelta{Sub: obs1.ZRem{Members: [][]byte{[]byte("m")}}}}, "no sorted set exists"},
		{"no-effect upsert", []obs1.Op{
			obs1.CollNew{Type: obs1.CollZSet},
			obs1.CollDelta{Sub: obs1.ZAdd{Entries: one}},
			obs1.CollDelta{Sub: obs1.ZAdd{Entries: one}},
		}, "already holds its score"},
		{"absent remove", []obs1.Op{
			obs1.CollNew{Type: obs1.CollZSet},
			obs1.CollDelta{Sub: obs1.ZAdd{Entries: one}},
			obs1.CollDelta{Sub: obs1.ZRem{Members: [][]byte{[]byte("ghost")}}},
		}, "is not in sorted set"},
		{"collnew zset consumed by sadd", []obs1.Op{
			obs1.CollNew{Type: obs1.CollZSet},
			obs1.CollDelta{Sub: obs1.SAdd{Members: [][]byte{[]byte("m")}}},
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

// TestApplierListPlane replays the emitter shapes writelog.go frames for
// lists: collnew plus a sided push on create, more pushes both ways, the
// positional lset, lrem, and lins surgery, decided-count pops, and the
// pop plus colldrop pair when the last element leaves.
func TestApplierListPlane(t *testing.T) {
	a, cx := newApplier(t)

	m := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	// Build a, b, c, d then operate: RPUSH b c, LPUSH a, RPUSH d.
	apply(t, a, 0, frame(t, 1, "l", obs1.CollNew{Type: obs1.CollList}))
	apply(t, a, 0, frame(t, 2, "l", obs1.CollDelta{Sub: obs1.RPush{Values: m("b", "c")}}))
	apply(t, a, 0, frame(t, 3, "l", obs1.CollDelta{Sub: obs1.LPush{Values: m("a")}}))
	apply(t, a, 0, frame(t, 4, "l", obs1.CollDelta{Sub: obs1.RPush{Values: m("d")}}))
	wantList(t, cx, "l", "a", "b", "c", "d")

	// LSET index 1 -> B, LINSERT so x lands at index 2, LREM positions 0 and 3.
	apply(t, a, 0, frame(t, 5, "l", obs1.CollDelta{Sub: obs1.LSet{Index: 1, Value: []byte("B")}}))
	apply(t, a, 0, frame(t, 6, "l", obs1.CollDelta{Sub: obs1.LIns{Index: 2, Value: []byte("x")}}))
	wantList(t, cx, "l", "a", "B", "x", "c", "d")
	apply(t, a, 0, frame(t, 7, "l", obs1.CollDelta{Sub: obs1.LRem{Indices: []uint32{0, 3}}}))
	wantList(t, cx, "l", "B", "x", "d")

	// Pop one from each end, then the emptying pop rides its colldrop.
	apply(t, a, 0, frame(t, 8, "l", obs1.CollDelta{Sub: obs1.LPop{Count: 1}}))
	apply(t, a, 0, frame(t, 9, "l", obs1.CollDelta{Sub: obs1.RPop{Count: 1}}))
	wantList(t, cx, "l", "x")
	apply(t, a, 0, frame(t, 10, "l", obs1.CollDelta{Sub: obs1.LPop{Count: 1}}))
	apply(t, a, 0, frame(t, 11, "l", obs1.CollDrop{}))
	if list.ReplayDrop(cx, []byte("l")) {
		t.Fatalf("l survived its colldrop")
	}

	apply(t, a, 0, frame(t, 12, "kd", obs1.CollNew{Type: obs1.CollList}))
	apply(t, a, 0, frame(t, 13, "kd", obs1.CollDelta{Sub: obs1.LPush{Values: m("v")}}))
	apply(t, a, 0, frame(t, 14, "kd", obs1.KeyDel{}))
	if list.ReplayDrop(cx, []byte("kd")) {
		t.Fatalf("the list survived its keydel")
	}
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got := a.Stats()
	if got.LPushes != 2 || got.RPushes != 2 || got.LPops != 2 || got.RPops != 1 ||
		got.LSets != 1 || got.LRems != 1 || got.LInserts != 1 || got.CollDrops != 1 || got.Dels != 1 {
		t.Fatalf("stats %+v, want the list plane counts", got)
	}
}

// wantList checks the replayed list's exact contents through a
// zero-effect probe: an lset writing each element back at its own index
// refuses only on divergence, and the length pin comes from an
// out-of-range lset refusing. Kept test-local so the production surface
// stays the replay entry points.
func wantList(t *testing.T, cx *shard.Ctx, key string, elems ...string) {
	t.Helper()
	for i, e := range elems {
		if err := list.ReplaySet(cx, []byte(key), int64(i), []byte(e)); err != nil {
			t.Fatalf("list %q index %d: %v", key, i, err)
		}
	}
	if err := list.ReplaySet(cx, []byte(key), int64(len(elems)), []byte("x")); err == nil {
		t.Fatalf("list %q is longer than %d elements", key, len(elems))
	}
}

// TestApplierListCorruptionIsLoud drives the divergences the list plane
// must refuse: deltas on a missing list, a pop the list cannot cover, an
// out-of-range lset and lins, and lrem positions out of range or out of
// order.
func TestApplierListCorruptionIsLoud(t *testing.T) {
	one := [][]byte{[]byte("v")}
	build := []obs1.Op{
		obs1.CollNew{Type: obs1.CollList},
		obs1.CollDelta{Sub: obs1.RPush{Values: [][]byte{[]byte("a"), []byte("b")}}},
	}
	cases := []struct {
		name string
		ops  []obs1.Op
		want string
	}{
		{"push on missing list", []obs1.Op{obs1.CollDelta{Sub: obs1.LPush{Values: one}}}, "no list exists"},
		{"pop on missing list", []obs1.Op{obs1.CollDelta{Sub: obs1.LPop{Count: 1}}}, "no list exists"},
		{"oversized pop", append(build[:2:2], obs1.CollDelta{Sub: obs1.RPop{Count: 3}}), "pop of 3"},
		{"lset out of range", append(build[:2:2], obs1.CollDelta{Sub: obs1.LSet{Index: 2, Value: one[0]}}), "outside list"},
		{"lins out of range", append(build[:2:2], obs1.CollDelta{Sub: obs1.LIns{Index: 3, Value: one[0]}}), "outside list"},
		// The encoder refuses out-of-order lrem indices before a frame
		// exists, so only the out-of-range form can reach the applier.
		{"lrem position out of range", append(build[:2:2], obs1.CollDelta{Sub: obs1.LRem{Indices: []uint32{2}}}), "is invalid"},
		{"collnew list consumed by sadd", []obs1.Op{
			obs1.CollNew{Type: obs1.CollList},
			obs1.CollDelta{Sub: obs1.SAdd{Members: one}},
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

// TestApplierStreamPlane replays the emitter shapes writelog.go frames
// for stream entries: collnew plus an xadd on create, more xadds at
// owner-assigned ids, the xadd plus xtrim run the trim clause frames,
// a standalone xtrim, an xdel of removed ids, and xsetid's
// unconditional assignment.
func TestApplierStreamPlane(t *testing.T) {
	a, cx := newApplier(t)

	kv := func(ss ...string) []obs1.FieldValue {
		out := make([]obs1.FieldValue, len(ss)/2)
		for i := range out {
			out[i] = obs1.FieldValue{Field: []byte(ss[2*i]), Value: []byte(ss[2*i+1])}
		}
		return out
	}
	apply(t, a, 0, frame(t, 1, "st", obs1.CollNew{Type: obs1.CollStream}))
	apply(t, a, 0, frame(t, 2, "st", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 1, IDSeq: 1, Pairs: kv("f1", "v1")}}))
	apply(t, a, 0, frame(t, 3, "st", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 1, IDSeq: 2, Pairs: kv("f2", "v2")}}))
	apply(t, a, 0, frame(t, 4, "st", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 2, IDSeq: 1, Pairs: kv("a", "1", "b", "2")}}))
	wantStream(t, cx, "st", 3, 2, 1)

	// XADD with a trim clause frames the entry then the trim, one run.
	apply(t, a, 0, frame(t, 5, "st", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 3, IDSeq: 0, Pairs: kv("f4", "v4")}}))
	apply(t, a, 0, frame(t, 6, "st", obs1.CollDelta{Sub: obs1.XTrim{Count: 1}}))
	wantStream(t, cx, "st", 3, 3, 0)

	// XDEL of 1-2, then a standalone XTRIM dropping the next-oldest 2-1.
	apply(t, a, 0, frame(t, 7, "st", obs1.CollDelta{Sub: obs1.XDel{IDMs: []uint64{1}, IDSeq: []uint64{2}}}))
	apply(t, a, 0, frame(t, 8, "st", obs1.CollDelta{Sub: obs1.XTrim{Count: 1}}))
	wantStream(t, cx, "st", 1, 3, 0)

	// XSETID assigns all three values; a later xadd lands above the new last id.
	apply(t, a, 0, frame(t, 9, "st", obs1.CollDelta{Sub: obs1.XSetID{LastMs: 10, LastSeq: 5, EntriesAdded: 9, MaxDelMs: 3, MaxDelSeq: 0}}))
	wantStream(t, cx, "st", 1, 10, 5)
	apply(t, a, 0, frame(t, 10, "st", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 11, IDSeq: 0, Pairs: kv("f5", "v5")}}))
	wantStream(t, cx, "st", 2, 11, 0)

	// An emptied stream persists: no colldrop arm exists, only keydel removes one.
	apply(t, a, 0, frame(t, 11, "kd", obs1.CollNew{Type: obs1.CollStream}))
	apply(t, a, 0, frame(t, 12, "kd", obs1.CollDelta{Sub: obs1.XAdd{IDMs: 1, IDSeq: 1, Pairs: kv("f", "v")}}))
	apply(t, a, 0, frame(t, 13, "kd", obs1.KeyDel{}))
	if stream.ReplayDrop(cx, []byte("kd")) {
		t.Fatalf("the stream survived its keydel")
	}
	if err := a.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got := a.Stats()
	if got.XAdds != 6 || got.XDels != 1 || got.XTrims != 2 || got.XSetIDs != 1 ||
		got.CollNews != 2 || got.Dels != 1 {
		t.Fatalf("stats %+v, want the stream plane counts", got)
	}
}

// wantStream pins the replayed stream's live length and last id through
// non-mutating strictness refusals: an oversized xtrim names the live
// count it cannot cover, and an xadd at id 0-0 names the last id it
// fails to exceed. Kept test-local so the production surface stays the
// replay entry points; the socket test proves entry contents by XRANGE.
func wantStream(t *testing.T, cx *shard.Ctx, key string, length uint64, lastMs, lastSeq uint64) {
	t.Helper()
	err := stream.ReplayXTrim(cx, []byte(key), 1<<62)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("of %d live entries", length)) {
		t.Fatalf("stream %q length pin: %v, want %d live entries", key, err, length)
	}
	probe := [][]byte{[]byte("f"), []byte("v")}
	err = stream.ReplayXAdd(cx, []byte(key), 0, 0, probe, false)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("last id %d-%d", lastMs, lastSeq)) {
		t.Fatalf("stream %q last id pin: %v, want %d-%d", key, err, lastMs, lastSeq)
	}
}

// TestApplierStreamCorruptionIsLoud drives the divergences the stream
// plane must refuse: deltas on a missing stream, an xadd at or below
// the last id, an xdel of an id that is not live, and a trim count the
// stream cannot cover.
func TestApplierStreamCorruptionIsLoud(t *testing.T) {
	one := []obs1.FieldValue{{Field: []byte("f"), Value: []byte("v")}}
	build := []obs1.Op{
		obs1.CollNew{Type: obs1.CollStream},
		obs1.CollDelta{Sub: obs1.XAdd{IDMs: 5, IDSeq: 5, Pairs: one}},
	}
	cases := []struct {
		name string
		ops  []obs1.Op
		want string
	}{
		{"xadd on missing stream", []obs1.Op{obs1.CollDelta{Sub: obs1.XAdd{IDMs: 1, IDSeq: 1, Pairs: one}}}, "no stream exists"},
		{"xdel on missing stream", []obs1.Op{obs1.CollDelta{Sub: obs1.XDel{IDMs: []uint64{1}, IDSeq: []uint64{1}}}}, "no stream exists"},
		{"xsetid on missing stream", []obs1.Op{obs1.CollDelta{Sub: obs1.XSetID{LastMs: 1}}}, "no stream exists"},
		{"xadd below last id", append(build[:2:2], obs1.CollDelta{Sub: obs1.XAdd{IDMs: 5, IDSeq: 5, Pairs: one}}), "does not exceed last id"},
		{"xdel of a dead id", append(build[:2:2], obs1.CollDelta{Sub: obs1.XDel{IDMs: []uint64{9}, IDSeq: []uint64{9}}}), "is not live"},
		{"oversized xtrim", append(build[:2:2], obs1.CollDelta{Sub: obs1.XTrim{Count: 5}}), "xtrim of 5"},
		{"collnew stream consumed by sadd", []obs1.Op{
			obs1.CollNew{Type: obs1.CollStream},
			obs1.CollDelta{Sub: obs1.SAdd{Members: [][]byte{[]byte("m")}}},
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

// TestApplierRefusesUnwiredKinds keeps the loud refusal for the one
// vocabulary that has not landed: the consumer group sub-ops.
func TestApplierRefusesUnwiredKinds(t *testing.T) {
	a, _ := newApplier(t)
	gd := obs1.GroupDelta{Sub: obs1.GNew{Group: []byte("g")}}
	if err := a.Apply(0, frame(t, 1, "st", gd)); err == nil || !strings.Contains(err.Error(), "not wired yet") {
		t.Fatalf("groupdelta: %v", err)
	}
}
