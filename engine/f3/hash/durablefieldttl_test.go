package hash

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The hash field-TTL round trips (spec 2064/f3/M8-collection-durability-plan, the field-
// TTL follow-on to the hash vertical): a per-field HEXPIRE deadline set or cleared after
// the last checkpoint must survive a reopen on the effect tail alone, and one taken at
// checkpoint time must ride the snapshot header. They are the field-level sibling of the
// key-expire round trip in durable_test.go, one nesting level down.
//
// The command handlers write their reply through a shard.Reply a package unit test cannot
// build, so hfexpireD and hfpersistD mirror the instrumented loops in expireGeneric and
// Hpersist exactly: apply the change through the same applyExpiry and persistField the
// handler uses, then cut the effect the returned status code selects.

// fieldTTLStore opens a store over an .aki handle, the shared open used by every reopen
// in this file.
func fieldTTLStore(t *testing.T, f *akifile.File) *store.Store {
	t.Helper()
	s, err := store.Open(store.Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	return s
}

// hfexpireD mirrors one field of HEXPIRE and its siblings: apply the deadline through the
// real applyExpiry and cut the resolved effect the way expireGeneric's loop does, a field
// deadline for a set and a field-delete for a set-to-the-past. It returns the status code.
func hfexpireD(cx *shard.Ctx, g *reg, key, field string, at int64) int64 {
	k := []byte(key)
	h := g.live(cx, k)
	if h == nil {
		return -2
	}
	code := applyExpiry(h, []byte(field), at, condNone, uint64(cx.NowMs))
	switch code {
	case 1:
		logFieldExpire(cx, k, []byte(field), at)
	case 2:
		logDelField(cx, k, []byte(field))
	}
	if h.card() == 0 {
		g.drop(k)
	} else {
		g.note(h)
	}
	return code
}

// hfpersistD mirrors one field of HPERSIST: clear the field TTL through the real
// persistField and cut a zero-deadline field-expire when it actually cleared one.
func hfpersistD(cx *shard.Ctx, g *reg, key, field string) int64 {
	k := []byte(key)
	h := g.live(cx, k)
	if h == nil {
		return -2
	}
	code := persistField(h, []byte(field))
	if code == 1 {
		logFieldExpire(cx, k, []byte(field), 0)
	}
	return code
}

// fieldTTLsOf reads a hash back to a field->deadline map for comparison, listing only the
// fields that carry a live TTL, across either band.
func fieldTTLsOf(h *hash) map[string]uint64 {
	out := map[string]uint64{}
	if h == nil {
		return out
	}
	h.each(func(field, value []byte) {
		if exp := h.fieldExp(field); exp != 0 {
			out[string(field)] = exp
		}
	})
	return out
}

// TestHashFieldExpireEffectLogRecovers is the effect-tail-only field-TTL round trip: a
// field deadline set, a set-then-cleared field, an untouched field, and a set-to-the-past
// that deleted a field all replay from the log with no snapshot, and the recovered hash
// carries the exact per-field deadlines the first run left.
func TestHashFieldExpireEffectLogRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashfieldttl.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := fieldTTLStore(t, f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	const futureA = int64(5_000_000)
	const futureB = int64(6_000_000)
	hsetD(cx, g, "h", "a", "1", "b", "2", "c", "3", "d", "4")
	hfexpireD(cx, g, "h", "a", futureA) // a takes a TTL
	hfpersistD(cx, g, "h", "a")         // then clears it, so the pair must land cleared on replay
	hfexpireD(cx, g, "h", "b", futureB) // b keeps its TTL
	hfexpireD(cx, g, "h", "d", 1)       // set-to-the-past deletes d (deadline <= NowMs)

	wantFields := fieldsOf(g.m["h"])
	wantTTLs := fieldTTLsOf(g.m["h"])
	if !reflect.DeepEqual(wantFields, map[string]string{"a": "1", "b": "2", "c": "3"}) {
		t.Fatalf("first-run fields = %v (d must be gone)", wantFields)
	}
	if !reflect.DeepEqual(wantTTLs, map[string]uint64{"b": uint64(futureB)}) {
		t.Fatalf("first-run field TTLs = %v, want only b", wantTTLs)
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
	s2 := fieldTTLStore(t, f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := fieldsOf(g2.m["h"]); !reflect.DeepEqual(got, wantFields) {
		t.Fatalf("fields after recovery = %v, want %v", got, wantFields)
	}
	if got := fieldTTLsOf(g2.m["h"]); !reflect.DeepEqual(got, wantTTLs) {
		t.Fatalf("field TTLs after recovery = %v, want %v (set-then-clear and set must both survive)", got, wantTTLs)
	}
}

// TestHashFieldExpireSnapshotRecovers is the field-TTL round trip across a checkpoint: the
// field deadlines live at snapshot time ride the snapshot header, then a tail of an
// HPERSIST on a snapshot-carried field and a fresh HEXPIRE on another replays on top, so
// the recovered state is the composition. Field b, untouched after the snapshot, proves
// the header carries a field TTL on its own.
func TestHashFieldExpireSnapshotRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashfieldttlsnap.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := fieldTTLStore(t, f)
	cx := &shard.Ctx{St: s, NowMs: 1}
	g := registry(cx)

	const carriedA = int64(5_000_000)
	const carriedB = int64(6_000_000)
	const futureC = int64(7_000_000)
	hsetD(cx, g, "h", "a", "1", "b", "2", "c", "3")
	hfexpireD(cx, g, "h", "a", carriedA)
	hfexpireD(cx, g, "h", "b", carriedB)

	Snapshot(cx) // folds a's and b's deadlines into the header field-TTL section

	hfpersistD(cx, g, "h", "a")         // clears a's snapshot-carried TTL
	hfexpireD(cx, g, "h", "c", futureC) // c takes a fresh TTL after the snapshot

	wantFields := fieldsOf(g.m["h"])
	wantTTLs := fieldTTLsOf(g.m["h"])
	if !reflect.DeepEqual(wantTTLs, map[string]uint64{"b": uint64(carriedB), "c": uint64(futureC)}) {
		t.Fatalf("first-run field TTLs = %v, want b and c", wantTTLs)
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
	s2 := fieldTTLStore(t, f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 1}
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	if got := fieldsOf(g2.m["h"]); !reflect.DeepEqual(got, wantFields) {
		t.Fatalf("fields after recovery = %v, want %v", got, wantFields)
	}
	if got := fieldTTLsOf(g2.m["h"]); !reflect.DeepEqual(got, wantTTLs) {
		t.Fatalf("field TTLs after recovery = %v, want %v (header b, tail persist a, tail set c)", got, wantTTLs)
	}
}

// TestHashFieldExpireReapOnRecovery proves the lazy reap reproduces on the recovered hash
// without any logged field-delete: a field whose deadline is durable but has passed by the
// recovery clock is installed by replay and reaped on the first access, exactly as it fired
// on the live run, the same reasoning the key-expire slice uses.
func TestHashFieldExpireReapOnRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashfieldreap.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := fieldTTLStore(t, f)
	cx := &shard.Ctx{St: s, NowMs: 100}
	g := registry(cx)

	hsetD(cx, g, "h", "a", "1", "b", "2")
	hfexpireD(cx, g, "h", "a", 200) // future at the write clock (100), past at the recovery clock

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
	s2 := fieldTTLStore(t, f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })
	cx2 := &shard.Ctx{St: s2, NowMs: 300} // past a's deadline
	if err := Recover(cx2); err != nil {
		t.Fatalf("recover: %v", err)
	}
	g2 := registry(cx2)

	// The first access at the later clock reaps the fired field, dropping a and leaving b.
	h := g2.live(cx2, []byte("h"))
	if h == nil {
		t.Fatal("hash should stay live, only its expired field a should reap")
	}
	if got := fieldsOf(h); !reflect.DeepEqual(got, map[string]string{"b": "2"}) {
		t.Fatalf("after reap on recovery = %v, want only b (a fired past its durable deadline)", got)
	}
}
