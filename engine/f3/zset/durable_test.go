package zset

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The zset durability round trip (spec 2064/f3/M8-collection-durability-plan, the zset
// arm of slices 2 and 3): run a mix of adds, score moves, removes, a pop, a range
// removal, and a delete against a zset store backed by a real .aki, close the file so
// only its durable bytes remain, reopen into a fresh store and an empty registry, replay
// the log, and assert every zset reads back exactly as the first run left it. It is the
// zset sibling of the set and hash verticals: a crash leaves nothing in the registry, and
// the log alone rebuilds the zsets.
//
// The command handlers write their reply through a shard.Reply a package unit test cannot
// build, so the zaddD, zremD, zpopminD, and zremrangebyrankD helpers mirror the handlers'
// mutate-and-log bodies exactly, the same convention the set saddD and hash hsetD helpers
// use. The delete goes through the real exported Delete, which logs its own key-delete
// effect.

// pair is a score-member input to the zadd driver, in ZADD's score-then-member order.
type pair struct {
	member string
	score  float64
}

// zaddD mirrors Zadd (no flags): create on the first written member, apply each pair, and
// cut an add effect for each member whose value changed.
func zaddD(cx *shard.Ctx, g *reg, key string, pairs ...pair) {
	k := []byte(key)
	z := g.live(cx, k)
	created := false
	if z == nil {
		z = newZset()
		created = true
	}
	for _, p := range pairs {
		gotAdded, gotChanged, newScore, _, _ := z.update([]byte(p.member), p.score, flags{})
		if gotAdded || gotChanged {
			logAdd(cx, k, []byte(p.member), newScore)
		}
	}
	if z.card() == 0 {
		if !created {
			g.drop(k)
		}
	} else {
		if created {
			g.m[key] = z
		}
		g.note(z)
	}
}

// zremD mirrors Zrem: remove each member, cut a remove effect for each that was present,
// and drop the key when the last member leaves.
func zremD(cx *shard.Ctx, g *reg, key string, members ...string) {
	k := []byte(key)
	z, _ := g.lookup(cx, k)
	if z == nil {
		return
	}
	for _, m := range members {
		if z.rem([]byte(m)) {
			logRemove(cx, k, []byte(m))
		}
	}
	if z.card() == 0 {
		g.drop(k)
	} else {
		g.note(z)
	}
}

// zpopminD mirrors the single-member Zpopmin: pop the lowest-scored member, cut a remove
// effect for it, drop the key when it empties, and return the popped member.
func zpopminD(cx *shard.Ctx, g *reg, key string) string {
	k := []byte(key)
	z, _ := g.lookup(cx, k)
	if z == nil {
		return ""
	}
	var got string
	z.pop(true, 1, func(m []byte, _ float64) {
		logRemove(cx, k, m)
		got = string(m)
	})
	if z.card() == 0 {
		g.drop(k)
	} else {
		g.note(z)
	}
	return got
}

// zremrangebyrankD mirrors Zremrangebyrank over an inclusive rank window: log every
// member in the resolved window, delete it, and drop the key when it empties.
func zremrangebyrankD(cx *shard.Ctx, g *reg, key string, start, stop int) {
	k := []byte(key)
	z, _ := g.lookup(cx, k)
	if z == nil {
		return
	}
	lo, hi, empty := clampRange(start, stop, z.card())
	if empty {
		return
	}
	logRemoveWindow(cx, k, z, lo, hi+1)
	z.removeRange(lo, hi+1)
	if z.card() == 0 {
		g.drop(k)
	} else {
		g.note(z)
	}
}

// durMS is one member and its score, the readback shape the round-trip asserts on.
type durMS struct {
	member string
	score  float64
}

// membersScores reads a zset back in ascending zset order, the ground truth the first run
// records and the reopen must reproduce.
func membersScores(z *zset) []durMS {
	if z == nil {
		return nil
	}
	out := make([]durMS, 0, z.card())
	z.forEach(func(m []byte, s float64) bool {
		out = append(out, durMS{member: string(m), score: s})
		return true
	})
	return out
}

func zsetDurStore(t *testing.T, path string, create bool) (*akifile.File, *store.Store) {
	t.Helper()
	var f *akifile.File
	var err error
	if create {
		f, err = akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	} else {
		f, err = akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	}
	if err != nil {
		t.Fatalf("aki open/create: %v", err)
	}
	s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	return f, s
}

func TestZsetEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zsetdur.aki")

	// First run: build several zsets across both bands, move a score, remove a member,
	// pop the min, remove a rank window, and delete a whole key.
	f, s := zsetDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	zaddD(cx, g, "scores", pair{"a", 1}, pair{"b", 2}, pair{"c", 3})
	zaddD(cx, g, "scores", pair{"b", 20}) // b moves to a higher score
	zaddD(cx, g, "nums", pair{"1", 1.5}, pair{"2", 2.5}, pair{"3", 3.5}, pair{"4", 4.5})
	zremD(cx, g, "scores", "a")           // scores -> c(3), b(20)
	_ = zpopminD(cx, g, "nums")           // nums loses its lowest, "1"
	zremrangebyrankD(cx, g, "nums", 0, 0) // nums loses its new lowest, "2"

	// A zset large enough to sit in the native band, to exercise the promotion path on
	// the rebuild.
	big := make([]pair, 200)
	for i := range big {
		big[i] = pair{member: fmt.Sprintf("m%03d", i), score: float64(i)}
	}
	zaddD(cx, g, "big", big...)
	if g.m["big"].enc != encSkiplist {
		t.Fatalf("big should be native, got %v", g.m["big"].enc)
	}

	zaddD(cx, g, "doomed", pair{"x", 9})
	if !Delete(cx, []byte("doomed")) {
		t.Fatal("delete of a live zset reported absent")
	}

	wantScores := membersScores(g.m["scores"])
	wantNums := membersScores(g.m["nums"])
	wantBig := membersScores(g.m["big"])
	if len(wantScores) != 2 || len(wantNums) != 2 || len(wantBig) != 200 {
		t.Fatalf("first-run cards: scores=%d nums=%d big=%d", len(wantScores), len(wantNums), len(wantBig))
	}
	if _, ok := g.m["doomed"]; ok {
		t.Fatal("doomed should be gone in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen into a fresh store and empty registry, rebuild from the effect
	// log alone.
	f2, s2 := zsetDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := membersScores(g2.m["scores"]); !reflect.DeepEqual(got, wantScores) {
		t.Fatalf("scores after recovery = %v, want %v", got, wantScores)
	}
	if got := membersScores(g2.m["nums"]); !reflect.DeepEqual(got, wantNums) {
		t.Fatalf("nums after recovery = %v, want %v", got, wantNums)
	}
	if got := membersScores(g2.m["big"]); !reflect.DeepEqual(got, wantBig) {
		t.Fatalf("big after recovery mismatched (len %d vs %d)", len(got), len(wantBig))
	}
	if _, ok := g2.m["doomed"]; ok {
		t.Fatal("doomed resurrected on recovery")
	}
}

func TestZsetSnapshotOnlyRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zsetsnap.aki")

	f, s := zsetDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	zaddD(cx, g, "a", pair{"x", 1}, pair{"y", 2})
	zaddD(cx, g, "b", pair{"p", 10}, pair{"q", 20}, pair{"r", 30})
	// A volatile zset: its key TTL rides the snapshot header.
	zaddD(cx, g, "vol", pair{"only", 5})
	g.m["vol"].expireAt = 999999

	// Fold every live zset to a snapshot frame.
	Snapshot(cx)

	wantA := membersScores(g.m["a"])
	wantB := membersScores(g.m["b"])
	wantVolTTL := g.m["vol"].expireAt

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := zsetDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := membersScores(g2.m["a"]); !reflect.DeepEqual(got, wantA) {
		t.Fatalf("a from snapshot = %v, want %v", got, wantA)
	}
	if got := membersScores(g2.m["b"]); !reflect.DeepEqual(got, wantB) {
		t.Fatalf("b from snapshot = %v, want %v", got, wantB)
	}
	if g2.m["vol"] == nil || g2.m["vol"].expireAt != wantVolTTL {
		t.Fatalf("vol TTL not restored from snapshot header: got %v want %v", g2.m["vol"], wantVolTTL)
	}
}

// zexpireD mirrors zsetBackend.Store for a future instant: set the live zset's deadline
// and cut the expire effect that carries it, the store arm of an EXPIRE to a future
// instant a unit test cannot drive through the reply-writing Expire.
func zexpireD(cx *shard.Ctx, g *reg, key string, at int64) {
	k := []byte(key)
	z := g.live(cx, k)
	if z == nil {
		return
	}
	z.expireAt = at
	logExpire(cx, k, at)
}

// zexpirePastD mirrors zsetBackend.Delete: an EXPIRE to a past instant drops the key on
// the spot and logs the key-delete so replay does not resurrect the members.
func zexpirePastD(cx *shard.Ctx, g *reg, key string) {
	k := []byte(key)
	logDeleteKey(cx, k)
	g.drop(k)
}

// TestZsetKeyExpireRecovers is the zset arm of the key-expire round trip: a deadline set
// or cleared after the snapshot must survive a reopen on the effect tail alone. It covers
// EXPIRE to a future instant, PERSIST of a snapshot-carried deadline, and EXPIRE to a past
// instant, mirroring the set vertical's TestSetKeyExpireRecovers.
func TestZsetKeyExpireRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zsetkeyexpire.aki")

	f, s := zsetDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	zaddD(cx, g, "future", pair{"a", 1}, pair{"b", 2})
	zaddD(cx, g, "persist", pair{"c", 3}, pair{"d", 4})
	zaddD(cx, g, "past", pair{"e", 5}, pair{"f", 6})
	const carried = int64(5_000_000)
	g.m["persist"].expireAt = carried

	Snapshot(cx)

	const future = int64(9_000_000)
	zexpireD(cx, g, "future", future)
	if !Persist(cx, []byte("persist")) {
		t.Fatal("persist of a zset with a deadline reported none removed")
	}
	zexpirePastD(cx, g, "past")

	wantFuture := membersScores(g.m["future"])
	wantPersist := membersScores(g.m["persist"])
	if g.m["future"].expireAt != future {
		t.Fatalf("first-run future TTL = %d, want %d", g.m["future"].expireAt, future)
	}
	if g.m["persist"].expireAt != 0 {
		t.Fatalf("first-run persist TTL = %d, want 0", g.m["persist"].expireAt)
	}
	if _, ok := g.m["past"]; ok {
		t.Fatal("past should be gone in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := zsetDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := membersScores(g2.m["future"]); !reflect.DeepEqual(got, wantFuture) {
		t.Fatalf("future members after recovery = %v, want %v", got, wantFuture)
	}
	if got := g2.m["future"].expireAt; got != future {
		t.Fatalf("future TTL after recovery = %d, want %d (post-snapshot expire effect must survive)", got, future)
	}
	if got := membersScores(g2.m["persist"]); !reflect.DeepEqual(got, wantPersist) {
		t.Fatalf("persist members after recovery = %v, want %v", got, wantPersist)
	}
	if got := g2.m["persist"].expireAt; got != 0 {
		t.Fatalf("persist TTL after recovery = %d, want 0 (post-snapshot PERSIST effect must survive)", got)
	}
	if _, ok := g2.m["past"]; ok {
		t.Fatal("past came back after recovery, the expire-past delete effect was lost")
	}
}

func TestZsetSnapshotThenEffectTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zsettail.aki")

	f, s := zsetDurStore(t, path, true)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	zaddD(cx, g, "k", pair{"a", 1}, pair{"b", 2}, pair{"c", 3})
	Snapshot(cx) // checkpoint: a snapshot frame supersedes the effects above

	// Effect tail cut after the snapshot: recovery folds the snapshot then replays these.
	zaddD(cx, g, "k", pair{"b", 22}) // b moves
	zremD(cx, g, "k", "a")           // a leaves
	zaddD(cx, g, "k", pair{"d", 4})  // d joins

	want := membersScores(g.m["k"])

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, s2 := zsetDurStore(t, path, false)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := membersScores(g2.m["k"]); !reflect.DeepEqual(got, want) {
		t.Fatalf("k after snapshot+tail = %v, want %v", got, want)
	}
}

func TestZsetEffectLogNoopWithoutFile(t *testing.T) {
	// A store with no .aki handle must take no durable effect: the log helpers no-op and
	// the in-memory path is unchanged.
	s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, Shard: 1})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)
	zaddD(cx, g, "k", pair{"a", 1}, pair{"b", 2})
	if got := membersScores(g.m["k"]); len(got) != 2 {
		t.Fatalf("in-memory zadd without a file: got %v", got)
	}
	// Signed zero collapses in the listpack band, matching a live ZADD.
	zaddD(cx, g, "z", pair{"nz", math.Copysign(0, -1)})
	if s, _ := g.m["z"].score([]byte("nz")); math.Signbit(s) {
		t.Fatal("listpack -0.0 should collapse to +0.0")
	}
}
