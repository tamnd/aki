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
