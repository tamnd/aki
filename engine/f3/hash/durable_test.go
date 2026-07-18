package hash

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The hash effect-log round trip (spec 2064/f3/M8-collection-durability-plan, the hash
// arm of slice 2): run a mix of sets, an overwrite, deletes, and a key-delete against a
// hash store backed by a real .aki, close the file so only its durable bytes remain,
// reopen into a fresh store and an empty registry, replay the effect log, and assert
// every hash reads back exactly as the first run left it. It is the hash sibling of the
// set vertical's TestSetEffectLogRecovers and the string vertical's
// TestReplayRebuildsIndex.
//
// The command handlers write their reply through a shard.Reply a package unit test
// cannot build, so hsetD, hsetnxD, and hdelD mirror the handlers' mutate-and-log bodies
// exactly, the way set's saddD mirrors Sadd. The key-delete goes through the real
// exported Delete, which logs its own key-delete effect.

// hsetD mirrors Hset: create on the first write, set each pair, and cut a set effect for
// every pair whether the field was new or overwritten.
func hsetD(cx *shard.Ctx, g *reg, key string, pairs ...string) {
	k := []byte(key)
	h := g.live(cx, k)
	if h == nil {
		h = newHash()
		g.m[key] = h
	}
	for i := 0; i < len(pairs); i += 2 {
		h.set([]byte(pairs[i]), []byte(pairs[i+1]))
		logSet(cx, k, []byte(pairs[i]), []byte(pairs[i+1]))
	}
	g.note(h)
}

// hsetnxD mirrors Hsetnx: set only when the field is absent, cutting a set effect only
// when it actually wrote.
func hsetnxD(cx *shard.Ctx, g *reg, key, field, value string) {
	k := []byte(key)
	h := g.live(cx, k)
	if h == nil {
		h = newHash()
		g.m[key] = h
	}
	if h.setNX([]byte(field), []byte(value)) {
		logSet(cx, k, []byte(field), []byte(value))
	}
	g.note(h)
}

// hdelD mirrors Hdel: delete each field, cut a field-delete effect for each one removed,
// and drop the key when the last field leaves.
func hdelD(cx *shard.Ctx, g *reg, key string, fields ...string) {
	k := []byte(key)
	h, _ := g.lookup(cx, k)
	if h == nil {
		return
	}
	for _, f := range fields {
		if h.del([]byte(f)) {
			logDelField(cx, k, []byte(f))
		}
	}
	if h.card() == 0 {
		g.drop(k)
	} else {
		g.note(h)
	}
}

// fieldsOf reads a hash back to a plain field->value map for comparison, across either
// band.
func fieldsOf(h *hash) map[string]string {
	out := map[string]string{}
	if h == nil {
		return out
	}
	h.each(func(field, value []byte) {
		out[string(field)] = string(value)
	})
	return out
}

func TestHashEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashdur.aki")
	openStore := func(f *akifile.File) *store.Store {
		s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	// First run: build several hashes, overwrite a field, delete a field, add through
	// HSETNX, and delete a whole key, then close so only durable bytes survive.
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := openStore(f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	hsetD(cx, g, "user", "name", "ada", "city", "london", "role", "eng")
	hsetD(cx, g, "user", "city", "paris")   // overwrite one field past its first value
	hdelD(cx, g, "user", "role")            // role leaves
	hsetnxD(cx, g, "user", "name", "grace") // refused, name already set
	hsetnxD(cx, g, "user", "team", "core")  // accepted, new field
	hsetD(cx, g, "counts", "a", "1", "b", "2")
	hsetD(cx, g, "doomed", "x", "y")
	if !Delete(cx, []byte("doomed")) { // doomed gone via a key-delete effect
		t.Fatal("delete of a live hash reported absent")
	}

	wantUser := fieldsOf(g.m["user"])
	wantCounts := fieldsOf(g.m["counts"])
	if !reflect.DeepEqual(wantUser, map[string]string{"name": "ada", "city": "paris", "team": "core"}) {
		t.Fatalf("first-run user = %v", wantUser)
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

	// Second run: reopen into a fresh store and empty registry, rebuild from the log.
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

	if got := fieldsOf(g2.m["user"]); !reflect.DeepEqual(got, wantUser) {
		t.Fatalf("user after recovery = %v, want %v", got, wantUser)
	}
	if got := fieldsOf(g2.m["counts"]); !reflect.DeepEqual(got, wantCounts) {
		t.Fatalf("counts after recovery = %v, want %v", got, wantCounts)
	}
	if _, ok := g2.m["doomed"]; ok {
		t.Fatal("deleted hash doomed came back after recovery")
	}
}

// TestHashEffectLogEmptiedKeyStaysGone proves a hash the effect tail empties by deleting
// its last field is dropped on replay, not left as an empty husk: the last field-delete
// takes the key with it, the same last-field-leaves rule the live command follows.
func TestHashEffectLogEmptiedKeyStaysGone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashempty.aki")
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
	hsetD(cx, g, "h", "only", "v")
	hdelD(cx, g, "h", "only") // empties h, so it drops
	if _, ok := g.m["h"]; ok {
		t.Fatal("h should be gone in the first run")
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
	s2, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f2, Shard: 1})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if _, ok := registry(cx2).m["h"]; ok {
		t.Fatal("emptied hash h came back after recovery")
	}
}

// TestHashEffectLogNoopWithoutFile proves the log helpers and Recover are inert on a
// plain in-memory store with no .aki handle: the mutations still land in the registry,
// nothing is logged, and Recover walks nothing and rebuilds nothing.
func TestHashEffectLogNoopWithoutFile(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	hsetD(cx, g, "h", "a", "1", "b", "2")
	if got := fieldsOf(g.m["h"]); !reflect.DeepEqual(got, map[string]string{"a": "1", "b": "2"}) {
		t.Fatalf("in-memory hash = %v", got)
	}
	cx2 := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover on a no-file store: %v", err)
	}
	if Len(cx2) != 0 {
		t.Fatalf("recover over a no-file store built %d hashes, want 0", Len(cx2))
	}
}
