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

// TestHashSnapshotRecovers is the slice-3 round trip across a checkpoint boundary (the
// hash arm of spec 2064/f3/M8-collection-durability-plan slice 3): build hashes with
// effects, fold them to snapshot frames the way the checkpoint dumper does, then mutate
// past the snapshot, close, reopen, and recover. The reopen must rebuild each hash from
// its snapshot and replay only the effect tail cut after it, so the recovered state is
// the composition of the snapshot and the later effects, and a key TTL taken at
// snapshot time survives. It also proves a snapshot-restored key an effect empties is
// dropped, and that the pre-snapshot effects do not leak past the snapshot reset.
func TestHashSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashsnap.aki")
	openStore := func(f *akifile.File) *store.Store {
		s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := openStore(f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	hsetD(cx, g, "user", "name", "ada", "city", "london")
	hsetD(cx, g, "counts", "a", "1", "b", "2")
	hsetD(cx, g, "gone", "x", "1", "y", "2")
	const ttl = int64(5_000_000) // far past NowMs, so the hash is live at snapshot and recovery
	g.m["user"].expireAt = ttl

	Snapshot(cx) // fold every live hash to a snapshot frame, the checkpoint dumper

	// Effects after the snapshot: user overwrites city and drops name, counts swaps a
	// field, and gone is emptied so its snapshot-restored form must drop on replay.
	hsetD(cx, g, "user", "city", "paris")
	hdelD(cx, g, "user", "name")
	hdelD(cx, g, "counts", "a")
	hsetD(cx, g, "counts", "c", "3")
	hdelD(cx, g, "gone", "x", "y")

	wantUser := fieldsOf(g.m["user"])     // {city: paris}
	wantCounts := fieldsOf(g.m["counts"]) // {b: 2, c: 3}
	if _, ok := g.m["gone"]; ok {
		t.Fatal("gone should be empty in the first run")
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

	if got := fieldsOf(g2.m["user"]); !reflect.DeepEqual(got, wantUser) {
		t.Fatalf("user after recovery = %v, want %v", got, wantUser)
	}
	if got := fieldsOf(g2.m["counts"]); !reflect.DeepEqual(got, wantCounts) {
		t.Fatalf("counts after recovery = %v, want %v", got, wantCounts)
	}
	if _, ok := g2.m["gone"]; ok {
		t.Fatal("gone came back after recovery, snapshot restored a key the tail emptied")
	}
	if got := g2.m["user"].expireAt; got != ttl {
		t.Fatalf("user TTL after recovery = %d, want %d (snapshot header must carry it)", got, ttl)
	}
}

// TestHashSnapshotOnlyRecovers proves the snapshot alone rebuilds a hash with no effect
// tail after it, the bounded path a clean shutdown followed by a reopen takes: the
// checkpoint holds the whole hash, the effect log past it is empty, and recovery reads
// the hash back from the snapshot frame only.
func TestHashSnapshotOnlyRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashsnaponly.aki")
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
	hsetD(cx, g, "h", "a", "1", "b", "2", "c", "3")
	Snapshot(cx)
	want := fieldsOf(g.m["h"])
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
	if got := fieldsOf(registry(cx2).m["h"]); !reflect.DeepEqual(got, want) {
		t.Fatalf("hash after snapshot-only recovery = %v, want %v", got, want)
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
