package zset

import (
	"bytes"
	"path/filepath"
	"sort"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/tier"
)

// The zset cold chunk store-side encoding and directory-backed reader (spec
// 2064/f3/06 sections 6 and 7, milestones/M7-slice-cold-chunk-zset-plan.md, PR D1).
// These tests hold the scaffold's three contracts before the demote pass (D2)
// drives it: a locator round-trips through the tierCold flag without touching the
// slot or entry fields, a member packed into a chunk reads back byte-identical
// through its locator, and the score-key-plus-member discriminator orders the
// directory in the zset's (score, member) order so a range read locates a cold
// band by one directory search.

// coldStore opens a cold-configured store, the region AppendChunk and ReadChunk
// need. The cap is irrelevant here (nothing demotes in a scaffold test); it just
// has to be a store with a live cold region.
func coldStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         1 << 20,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open cold store: %v", err)
	}
	return st
}

// TestLocatorRoundTrip proves the locator codec: packLoc composes a slot and an
// entry, locSlot and locEntry recover them, and the tierCold flag rides bit 31
// without leaking into either field (the resident path masks it off, so a cold loc
// and a slab offset can never be confused).
func TestLocatorRoundTrip(t *testing.T) {
	cases := []struct{ slot, entry uint32 }{
		{0, 0},
		{1, 1},
		{7, maxChunkEntry - 1},
		{(1 << coldSlotBits) - 1, maxChunkEntry - 1},
		{123, 456},
	}
	for _, c := range cases {
		loc := packLoc(c.slot, c.entry)
		if loc&tierCold != 0 {
			t.Fatalf("packLoc(%d,%d) set the tier bit: %#x", c.slot, c.entry, loc)
		}
		if got := locSlot(loc); got != c.slot {
			t.Fatalf("locSlot(%#x) = %d, want %d", loc, got, c.slot)
		}
		if got := locEntry(loc); got != c.entry {
			t.Fatalf("locEntry(%#x) = %d, want %d", loc, got, c.entry)
		}
		// With the tier flag set, as a retiered record carries it, the fields must
		// still decode unchanged: locSlot masks the flag before shifting.
		flagged := loc | tierCold
		if got := locSlot(flagged); got != c.slot {
			t.Fatalf("locSlot(flagged %#x) = %d, want %d", flagged, got, c.slot)
		}
		if got := locEntry(flagged); got != c.entry {
			t.Fatalf("locEntry(flagged %#x) = %d, want %d", flagged, got, c.entry)
		}
	}
}

// coldSlotBits is the offset-table field width, the count of loc bits between the
// entry field and the tierCold flag. It lives in the test because the D1 scaffold
// does not yet pack (the demote pass in D2 introduces it in production), but the
// round-trip test needs the field's top value to prove the slot never overflows
// into the flag.
const coldSlotBits = 31 - coldEntryBits

// TestChunkReadback packs a run of members into cold chunks the way the demote
// pass will, then reads each back through its locator. It proves the store-side
// encoding (the packed-pair codec, AppendChunk) and the reader (member,
// PackedPairAt, the offset table) agree on the packed layout end to end.
func TestChunkReadback(t *testing.T) {
	st := coldStore(t)
	c := &coldChunks{st: st}

	// Members in (score, member) order, several per chunk, packed contiguously the
	// way a rank-window demote lays them down.
	type ent struct {
		score  uint64
		member []byte
	}
	// gen returns distinct members; the score is the index so the run is already in
	// (score, member) order, the way a rank-window demote gathers it.
	members := gen(0, 20, 8)
	var ents []ent
	for i := range members {
		ents = append(ents, ent{score: uint64(i), member: []byte(members[i])})
	}

	const perChunk = 7
	var locs []uint32
	key := []byte("z")
	for start := 0; start < len(ents); start += perChunk {
		end := start + perChunk
		if end > len(ents) {
			end = len(ents)
		}
		var pk store.ChunkPacker
		for j := start; j < end; j++ {
			pk.Add(ents[j].member, scoreBytes(ents[j].score), 0)
		}
		payload, flags := pk.Finish()
		disc := discOf(ents[start].score, ents[start].member)
		off, ok := c.st.AppendChunk(kindZsetScore, flags, uint16(end-start), key, disc, payload)
		if !ok {
			t.Fatalf("AppendChunk chunk starting at %d failed", start)
		}
		slot := uint32(len(c.offs))
		c.offs = append(c.offs, off)
		c.dir.Insert(disc, uint32(end-start), off)
		for j := start; j < end; j++ {
			locs = append(locs, packLoc(slot, uint32(j-start)))
		}
	}

	for i, loc := range locs {
		got, ok := c.member(loc)
		if !ok {
			t.Fatalf("member(%#x) for entry %d missed", loc, i)
		}
		if !bytes.Equal(got, ents[i].member) {
			t.Fatalf("entry %d: member(%#x) = %q, want %q", i, loc, got, ents[i].member)
		}
	}

	// An out-of-range slot is a clean miss, not a panic: the read paths treat it as
	// a torn locator.
	if _, ok := c.member(packLoc(uint32(len(c.offs)), 0)); ok {
		t.Fatal("member on an out-of-range slot should miss")
	}
	// An entry index past the chunk's element count is a clean miss too.
	if _, ok := c.member(packLoc(0, perChunk)); ok {
		t.Fatal("member on an out-of-range entry should miss")
	}
}

// TestDiscOrder proves the discriminator orders the directory in the zset's
// (score, member) order: a byte-lexicographic sort of the discriminators equals a
// sort by score then member, so tier.Directory's ordered search locates a cold
// member by score or by rank with no per-type comparator.
func TestDiscOrder(t *testing.T) {
	type pair struct {
		score  uint64
		member string
	}
	pairs := []pair{
		{5, "b"},
		{5, "a"},
		{1, "z"},
		{5, "aa"},
		{2, "a"},
		{1, "a"},
	}
	// The order the zset presents: score ascending, member ascending on a tie.
	want := append([]pair(nil), pairs...)
	sort.Slice(want, func(i, j int) bool {
		if want[i].score != want[j].score {
			return want[i].score < want[j].score
		}
		return want[i].member < want[j].member
	})
	// The order a byte-lexicographic sort of the discriminators presents.
	got := append([]pair(nil), pairs...)
	sort.Slice(got, func(i, j int) bool {
		di := discOf(got[i].score, []byte(got[i].member))
		dj := discOf(got[j].score, []byte(got[j].member))
		return bytes.Compare(di, dj) < 0
	})
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("disc order at %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestChunkEntryTorn proves the packed-pair walker rejects a truncated payload
// rather than reading past it: a truncated tail is a clean false, which a cold
// read reports as a miss.
func TestChunkEntryTorn(t *testing.T) {
	var pk store.ChunkPacker
	pk.Add([]byte("hello"), scoreBytes(7), 0)
	payload, flags := pk.Finish()
	if p, ok := store.PackedPairAt(payload, flags, 1, 0); !ok || string(p.Field) != "hello" {
		t.Fatalf("PackedPairAt(good,0) = %q,%v", p.Field, ok)
	}
	if _, ok := store.PackedPairAt(payload, flags, 1, 1); ok {
		t.Fatal("PackedPairAt past the last entry should miss")
	}
	// Cutting bytes off the tail while claiming the same count is torn.
	if _, ok := store.PackedPairAt(payload[:len(payload)-3], flags, 1, 0); ok {
		t.Fatal("PackedPairAt on a torn payload should miss")
	}
}

// TestColdResidentAndDirty covers the two directory-facing helpers: residentBytes
// counts the directory and the offset table (the cold state's own resident cost),
// and markDirty flags the chunk owning a discriminator without touching the frame.
func TestColdResidentAndDirty(t *testing.T) {
	st := coldStore(t)
	c := &coldChunks{st: st}
	if got := c.residentBytes(); got != 0 {
		t.Fatalf("empty cold state residentBytes = %d, want 0", got)
	}

	key := []byte("z")
	disc := discOf(3, []byte("m"))
	var pk store.ChunkPacker
	pk.Add([]byte("m"), scoreBytes(3), 0)
	payload, flags := pk.Finish()
	off, ok := c.st.AppendChunk(kindZsetScore, flags, 1, key, disc, payload)
	if !ok {
		t.Fatal("AppendChunk failed")
	}
	c.offs = append(c.offs, off)
	c.dir.Insert(disc, 1, off)
	if got := c.residentBytes(); got == 0 {
		t.Fatal("residentBytes should count the directory and the offset table once a chunk lands")
	}

	c.markDirty(disc)
	idx, ok := c.dir.Floor(disc)
	if !ok {
		t.Fatal("Floor should find the inserted descriptor")
	}
	if _, _, status := c.dir.At(idx); status&tier.DescDirty == 0 {
		t.Fatal("markDirty should set the dirty status bit")
	}
}
