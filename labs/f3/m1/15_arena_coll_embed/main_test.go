package main

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// oneMemberBlob packs the same listpack shape the engine and the lab use.
func TestOneMemberBlob(t *testing.T) {
	b := oneMemberBlob([]byte("hello"))
	if !bytes.Equal(b, []byte{5, 'h', 'h', 'e', 'l', 'l', 'o'}) {
		t.Fatalf("oneMemberBlob = %v", b)
	}
}

// The embed arm charges well under half the wall arm per tiny collection, the
// memory-bar claim, and it does so through the real PutCollBlob path. The bound
// is structural (packed off-heap record vs three GC-scanned heap objects), so a
// modest count keeps the test fast without making the margin fragile.
func TestEmbedClearsMemoryBar(t *testing.T) {
	member := []byte("hello")
	const count = 200_000
	wall := heapPerWall(count, member)
	emb := embedPerColl(count, member)
	if wall <= 0 {
		t.Fatalf("wall B/coll = %.1f, want positive", wall)
	}
	ratio := emb.total / wall
	if ratio > 0.5 {
		t.Fatalf("embed/wall = %.2fx (embed total %.1f B, wall %.1f B), want <= 0.50x",
			ratio, emb.total, wall)
	}
	// The record bytes live off the Go heap; only the index stays on it, a small
	// fraction of the wall's per-collection heap charge.
	if emb.indexBytes > 0.25*wall {
		t.Fatalf("embed index %.1f B/coll on heap, want << wall %.1f B/coll",
			emb.indexBytes, wall)
	}
}

// The embed arm's per-record arena charge matches the store's own accounting
// for the fixed-width key and single-member blob: header, aligned key, reserved
// value capacity.
func TestEmbedRecordCharge(t *testing.T) {
	s := store.New(8<<20, 1<<20)
	blob := oneMemberBlob([]byte("hello"))
	if err := s.PutCollBlob(key(0), 0x02, 1, blob, 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	rec, ok := s.MemoryUsage(key(0), 0)
	if !ok {
		t.Fatal("record missing")
	}
	// key(0) = "set:00000000" is 12 bytes -> align8 16; blob is 7 bytes ->
	// align8 8; header 16. No TTL slot.
	if want := uint64(16 + 16 + 8); rec != want {
		t.Fatalf("recBytes = %d, want %d", rec, want)
	}
}
