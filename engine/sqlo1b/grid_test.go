package sqlo1b

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGridLifecycle(t *testing.T) {
	g := NewGrid(8)
	ext, err := g.Allocate(KindVlog, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ext != 1 {
		t.Fatalf("first allocation got extent %d, want 1 (0 is the header extent)", ext)
	}
	if g.State(ext) != StateActive {
		t.Fatalf("state %s after allocate", g.State(ext))
	}

	sealed, err := g.Seal(KindVlog, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sealed != ext || g.State(ext) != StateSealed {
		t.Fatalf("seal returned %d state %s", sealed, g.State(ext))
	}

	if err := g.Free(ext, 10); err != nil {
		t.Fatal(err)
	}
	if g.State(ext) != StateQuarantined {
		t.Fatalf("state %s after free", g.State(ext))
	}
	if n := g.ReleaseQuarantine(9); n != 0 || g.State(ext) != StateQuarantined {
		t.Fatalf("released %d at durable 9, tag is 10; state %s", n, g.State(ext))
	}
	if n := g.ReleaseQuarantine(10); n != 1 || g.State(ext) != StateFree {
		t.Fatalf("released %d at durable 10; state %s", n, g.State(ext))
	}

	// Reuse before grow: the released extent is the lowest free again.
	got, err := g.Allocate(KindIndex, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != ext {
		t.Fatalf("reallocation got %d, want the released %d", got, ext)
	}
}

func TestOneActivePerStream(t *testing.T) {
	g := NewGrid(8)
	if _, err := g.Allocate(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Allocate(KindVlog, 0); err == nil {
		t.Fatal("second active on one stream allocated without error")
	}
	if _, err := g.Allocate(KindVlog, 1); err != nil {
		t.Fatalf("other shard blocked: %v", err)
	}
	if _, err := g.Allocate(KindIndex, 0); err != nil {
		t.Fatalf("other kind blocked: %v", err)
	}
	if _, err := g.Seal(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Allocate(KindVlog, 0); err != nil {
		t.Fatalf("stream still blocked after seal: %v", err)
	}
}

func TestIllegalTransitions(t *testing.T) {
	g := NewGrid(8)
	if _, err := g.Seal(KindVlog, 0); err == nil {
		t.Fatal("seal with no active extent")
	}
	ext, err := g.Allocate(KindVlog, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Free(ext, 1); err == nil {
		t.Fatal("freed an active extent")
	}
	if err := g.Free(5, 1); err == nil {
		t.Fatal("freed a free extent")
	}
	if err := g.Free(0, 1); err == nil {
		t.Fatal("freed the header extent")
	}
	if _, err := g.Seal(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if err := g.Free(ext, 1); err != nil {
		t.Fatal(err)
	}
	if err := g.Free(ext, 2); err == nil {
		t.Fatal("double free on a quarantined extent")
	}
	if _, err := g.Allocate(0, 0); err == nil {
		t.Fatal("allocated kind 0")
	}
}

func TestAllocateExhaustion(t *testing.T) {
	g := NewGrid(3)
	for _, shard := range []uint16{0, 1} {
		if _, err := g.Allocate(KindVlog, shard); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Allocate(KindVlog, 2); err == nil {
		t.Fatal("allocation past the grid succeeded")
	}
	g.Grow(2)
	if _, err := g.Allocate(KindVlog, 2); err != nil {
		t.Fatalf("allocation after grow: %v", err)
	}
	if g.ExtentCount() != 5 {
		t.Fatalf("extent count %d after grow, want 5", g.ExtentCount())
	}
}

// TestAllocmapRoundtrip packs a mixed grid and loads it back: every
// non-free state is a set bit, and non-free loads as sealed because
// the bitmap only records free versus not-free.
func TestAllocmapRoundtrip(t *testing.T) {
	g := NewGrid(20)
	a, _ := g.Allocate(KindVlog, 0)  // active
	s, _ := g.Allocate(KindIndex, 0) // sealed below
	q, _ := g.Allocate(KindVlog, 1)  // quarantined below
	if _, err := g.Seal(KindIndex, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Seal(KindVlog, 1); err != nil {
		t.Fatal(err)
	}
	if err := g.Free(q, 5); err != nil {
		t.Fatal(err)
	}

	m := g.Allocmap()
	if want := (20 + 7) / 8; len(m) != want {
		t.Fatalf("allocmap %d bytes, want %d", len(m), want)
	}
	loaded, err := LoadGrid(m, 20)
	if err != nil {
		t.Fatal(err)
	}
	for ext := range uint64(20) {
		wantFree := g.State(ext) == StateFree
		gotFree := loaded.State(ext) == StateFree
		if wantFree != gotFree {
			t.Fatalf("extent %d: free=%v loaded free=%v", ext, wantFree, gotFree)
		}
	}
	for _, ext := range []uint64{a, s, q} {
		if loaded.State(ext) != StateSealed {
			t.Fatalf("extent %d loads as %s, want sealed", ext, loaded.State(ext))
		}
	}
	if _, err := LoadGrid(m, 99); err == nil {
		t.Fatal("size mismatch accepted")
	}
}

// TestRebuildAllocmap pins the repair path and its conservatism: a
// verifying header means not-free, a zeroed or torn header means
// free, and a freed extent that still carries its stale header is
// kept not-free (the rebuild may leak space, never a used extent).
func TestRebuildAllocmap(t *testing.T) {
	const extentSize = 4096
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(6 * extentSize); err != nil {
		t.Fatal(err)
	}

	writeHdr := func(ext uint64, h *ExtentHeader) {
		if _, err := f.WriteAt(h.Encode(), int64(ext)*extentSize); err != nil {
			t.Fatal(err)
		}
	}
	writeHdr(1, &ExtentHeader{Kind: KindVlog, EFlags: EFlagSealed})
	writeHdr(3, &ExtentHeader{Kind: KindIndex}) // active tail, no seal bit
	torn := hdrFixture().Encode()[:32]
	if _, err := f.WriteAt(torn, 4*extentSize); err != nil {
		t.Fatal(err)
	}
	// Extent 5 was freed but keeps the header from its previous life.
	writeHdr(5, &ExtentHeader{Kind: KindDict, EFlags: EFlagSealed})

	m, err := RebuildAllocmap(f, extentSize, 6)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]bool{0: true, 1: true, 2: false, 3: true, 4: false, 5: true}
	for ext, alloc := range want {
		if got := m[ext/8]&(1<<(ext%8)) != 0; got != alloc {
			t.Fatalf("extent %d: rebuilt alloc=%v, want %v", ext, got, alloc)
		}
	}
}
