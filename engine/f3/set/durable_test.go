package set

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The set effect-log round trip (spec 2064/f3/M8-collection-durability-plan slice
// 2): run a mix of adds, removes, a pop, and a delete against a set store backed by
// a real .aki, close the file so only its durable bytes remain, reopen into a fresh
// store and an empty registry, replay the effect log, and assert every set reads
// back exactly as the first run left it. It is the set sibling of the string
// vertical's TestReplayRebuildsIndex: a crash leaves nothing in the registry, and
// the effect log alone rebuilds the sets.
//
// The command handlers write their reply through a shard.Reply a package unit test
// cannot build (the same limit expire_test names), so the saddD, sremD, and spopD
// helpers mirror the handlers' mutate-and-log bodies exactly, the way addKey mirrors
// Sadd and applyStore mirrors a STORE handler. The delete goes through the real
// exported Delete, which logs its own key-delete effect.

// saddD mirrors Sadd: create on the first member, add each, and cut an add effect
// for each member newly added.
func saddD(cx *shard.Ctx, g *reg, key string, members ...string) {
	k := []byte(key)
	s := g.live(cx, k)
	if s == nil {
		s = newSet([]byte(members[0]))
		g.m[key] = s
	}
	for _, m := range members {
		if s.add([]byte(m)) {
			logAdd(cx, k, []byte(m))
		}
	}
	g.note(s)
}

// sremD mirrors Srem: remove each, cut a remove effect for each member removed, and
// drop the key when the last member leaves.
func sremD(cx *shard.Ctx, g *reg, key string, members ...string) {
	k := []byte(key)
	s, _ := g.lookup(cx, k)
	if s == nil {
		return
	}
	for _, m := range members {
		if s.rem([]byte(m)) {
			logRemove(cx, k, []byte(m))
		}
	}
	if s.card() == 0 {
		g.drop(k)
	} else {
		g.note(s)
	}
}

// spopD mirrors the single-member Spop: draw one, cut a remove effect for the
// resolved member, drop the key when it empties, and return the popped member.
func spopD(cx *shard.Ctx, g *reg, key string) []byte {
	k := []byte(key)
	s, _ := g.lookup(cx, k)
	if s == nil {
		return nil
	}
	var sc [64]byte
	m := s.popOne(g, sc[:])
	logRemove(cx, k, m)
	out := append([]byte(nil), m...)
	if s.card() == 0 {
		g.drop(k)
	} else {
		g.note(s)
	}
	return out
}

// sexpireD mirrors setBackend.Store for a future instant: set the live set's deadline
// and cut the expire effect that carries it. Expire itself runs through expire.Apply and
// writes a reply a unit test cannot build, so this driver stands in for the store arm of
// an EXPIRE to a future instant.
func sexpireD(cx *shard.Ctx, g *reg, key string, at int64) {
	k := []byte(key)
	s := g.live(cx, k)
	if s == nil {
		return
	}
	s.expireAt = at
	logExpire(cx, k, at)
}

// sexpirePastD mirrors setBackend.Delete: an EXPIRE to a past instant drops the key on
// the spot and logs the key-delete so replay does not resurrect the members.
func sexpirePastD(cx *shard.Ctx, g *reg, key string) {
	k := []byte(key)
	logDeleteKey(cx, k)
	g.drop(k)
}

func TestSetEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setdur.aki")
	create := func() *akifile.File {
		f, err := akifile.Create(path, akifile.CreateOptions{
			ShardCount: 4,
			Sync:       akifile.SyncNo,
		})
		if err != nil {
			t.Fatalf("create aki: %v", err)
		}
		return f
	}
	openStore := func(f *akifile.File) *store.Store {
		s, err := store.Open(store.Options{
			ArenaBytes:  4 << 20,
			SegBytes:    1 << 20,
			AkiValueLog: f,
			Shard:       1,
		})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	// First run: build several sets, remove a member, pop a member, and delete a
	// whole key, then close the store and the file so only durable bytes survive.
	f := create()
	s := openStore(f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	saddD(cx, g, "colors", "red", "green", "blue")
	saddD(cx, g, "nums", "1", "2", "3", "4")
	sremD(cx, g, "colors", "green") // colors -> red, blue
	popped := spopD(cx, g, "nums")  // nums loses one resolved member
	saddD(cx, g, "letters", "a")
	if !Delete(cx, []byte("letters")) { // letters gone via a key-delete effect
		t.Fatal("delete of a live set reported absent")
	}

	// The live registry state the reopen must reproduce.
	wantColors := members(g.m["colors"])
	wantNums := members(g.m["nums"])
	if len(wantColors) != 2 || len(wantNums) != 3 {
		t.Fatalf("first-run state: colors=%v nums=%v", wantColors, wantNums)
	}
	if _, ok := g.m["letters"]; ok {
		t.Fatal("letters should be gone in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen the file into a fresh store and an empty registry, then
	// rebuild the sets from the effect log alone.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}

	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := members(g2.m["colors"]); !reflect.DeepEqual(got, wantColors) {
		t.Fatalf("colors after recovery = %v, want %v", got, wantColors)
	}
	if got := members(g2.m["nums"]); !reflect.DeepEqual(got, wantNums) {
		t.Fatalf("nums after recovery = %v, want %v", got, wantNums)
	}
	if g2.m["nums"].has(popped) {
		t.Fatalf("popped member %q resurfaced after recovery", popped)
	}
	if _, ok := g2.m["letters"]; ok {
		t.Fatal("deleted set letters came back after recovery")
	}
}

// TestSetSnapshotRecovers is the slice-3 round trip across a checkpoint boundary
// (spec 2064/f3/M8-collection-durability-plan slice 3): build sets with effects, fold
// them to snapshot frames the way the checkpoint dumper does, then mutate past the
// snapshot, close, reopen, and recover. The reopen must rebuild each set from its
// snapshot and replay only the effect tail cut after it, so the recovered state is
// the composition of the snapshot and the later effects, and a key TTL taken at
// snapshot time survives. It also proves a snapshot-restored key an effect empties is
// dropped, and that the pre-snapshot effects do not leak past the snapshot reset.
func TestSetSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setsnap.aki")
	create := func() *akifile.File {
		f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
		if err != nil {
			t.Fatalf("create aki: %v", err)
		}
		return f
	}
	openStore := func(f *akifile.File) *store.Store {
		s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	// First run: build sets with effects, give one a TTL, fold to snapshots, then
	// mutate past the snapshot so recovery must compose the snapshot with a tail.
	f := create()
	s := openStore(f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	saddD(cx, g, "colors", "red", "green", "blue")
	saddD(cx, g, "nums", "1", "2", "3")
	saddD(cx, g, "gone", "x", "y")
	const ttl = int64(5_000_000) // far past NowMs, so the set is live at snapshot and recovery
	g.m["colors"].expireAt = ttl

	Snapshot(cx) // fold every live set to a snapshot frame, the checkpoint dumper

	// Effects after the snapshot: colors loses green and gains yellow, nums swaps a
	// member, and gone is emptied so its snapshot-restored form must drop on replay.
	sremD(cx, g, "colors", "green")
	saddD(cx, g, "colors", "yellow")
	sremD(cx, g, "nums", "2")
	saddD(cx, g, "nums", "9")
	sremD(cx, g, "gone", "x", "y")

	wantColors := members(g.m["colors"]) // [blue red yellow]
	wantNums := members(g.m["nums"])     // [1 3 9]
	if _, ok := g.m["gone"]; ok {
		t.Fatal("gone should be empty in the first run")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen into a fresh store and empty registry, then recover from the
	// snapshot base plus the effect tail.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}

	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := members(g2.m["colors"]); !reflect.DeepEqual(got, wantColors) {
		t.Fatalf("colors after recovery = %v, want %v", got, wantColors)
	}
	if got := members(g2.m["nums"]); !reflect.DeepEqual(got, wantNums) {
		t.Fatalf("nums after recovery = %v, want %v", got, wantNums)
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("gone came back after recovery, snapshot restored a key the tail emptied")
	}
	if got := g2.m["colors"].expireAt; got != ttl {
		t.Fatalf("colors TTL after recovery = %d, want %d (snapshot header must carry it)", got, ttl)
	}
}

// TestSetSnapshotOnlyRecovers proves the snapshot alone rebuilds a set with no effect
// tail after it, the bounded path a clean shutdown followed by a reopen takes: the
// checkpoint holds the whole set, the effect log past it is empty, and recovery reads
// the set back from the snapshot frame only.
func TestSetSnapshotOnlyRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setsnaponly.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)
	saddD(cx, g, "s", "a", "b", "c")
	Snapshot(cx)
	want := members(g.m["s"])
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f2, Shard: 1})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := members(registry(cx2).m["s"]); !reflect.DeepEqual(got, want) {
		t.Fatalf("set after snapshot-only recovery = %v, want %v", got, want)
	}
}

// TestSetKeyExpireRecovers is the key-expire effect round trip (spec 2064/f3 M8 shared
// key-expire slice): a deadline set or cleared AFTER the snapshot must survive a reopen on
// the effect tail alone, not just a deadline the snapshot header captured. It covers three
// tails cut past a checkpoint: EXPIRE to a future instant (a fresh deadline where the
// snapshot had none), PERSIST (a snapshot-carried deadline cleared to none), and EXPIRE to
// a past instant (an immediate delete that must not resurrect the set on replay).
func TestSetKeyExpireRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setkeyexpire.aki")
	create := func() *akifile.File {
		f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
		if err != nil {
			t.Fatalf("create aki: %v", err)
		}
		return f
	}
	openStore := func(f *akifile.File) *store.Store {
		s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	f := create()
	s := openStore(f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	saddD(cx, g, "future", "a", "b")
	saddD(cx, g, "persist", "c", "d")
	saddD(cx, g, "past", "e", "f")
	const carried = int64(5_000_000) // far past NowMs, so the snapshot captures a live TTL
	g.m["persist"].expireAt = carried

	Snapshot(cx) // fold every live set, carrying persist's deadline into its header

	// Tails cut after the snapshot: future gains a deadline, persist drops the one its
	// snapshot header carried, and past expires on the spot.
	const future = int64(9_000_000)
	sexpireD(cx, g, "future", future)
	if !Persist(cx, []byte("persist")) {
		t.Fatal("persist of a set with a deadline reported none removed")
	}
	sexpirePastD(cx, g, "past")

	wantFuture := members(g.m["future"])
	wantPersist := members(g.m["persist"])
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

	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}

	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := members(g2.m["future"]); !reflect.DeepEqual(got, wantFuture) {
		t.Fatalf("future members after recovery = %v, want %v", got, wantFuture)
	}
	if got := g2.m["future"].expireAt; got != future {
		t.Fatalf("future TTL after recovery = %d, want %d (post-snapshot expire effect must survive)", got, future)
	}
	if got := members(g2.m["persist"]); !reflect.DeepEqual(got, wantPersist) {
		t.Fatalf("persist members after recovery = %v, want %v", got, wantPersist)
	}
	if got := g2.m["persist"].expireAt; got != 0 {
		t.Fatalf("persist TTL after recovery = %d, want 0 (post-snapshot PERSIST effect must survive)", got)
	}
	if _, ok := g2.m["past"]; ok {
		t.Fatal("past came back after recovery, the expire-past delete effect was lost")
	}
}

// TestSetEffectLogNoopWithoutFile proves the log helpers and Recover are inert on a
// plain in-memory store with no .aki handle: the mutations still land in the
// registry, nothing is logged, and Recover walks nothing and rebuilds nothing.
func TestSetEffectLogNoopWithoutFile(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	saddD(cx, g, "s", "a", "b")
	if got := members(g.m["s"]); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("in-memory set = %v, want [a b]", got)
	}
	// A fresh registry over the same store recovers nothing, since no effect frame
	// was ever cut.
	cx2 := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover on a no-file store: %v", err)
	}
	if Len(cx2) != 0 {
		t.Fatalf("recover over a no-file store built %d sets, want 0", Len(cx2))
	}
}
