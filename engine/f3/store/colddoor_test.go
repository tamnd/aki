package store

import (
	"fmt"
	"testing"
)

// TestColdDoorMarkAndTest is the doorkeeper's core: an unmarked key is absent, a
// marked key is present, and an unrelated key stays absent at low density.
func TestColdDoorMarkAndTest(t *testing.T) {
	d := newColdDoor(1 << 16)
	const fp = uint64(0x1234_5678)
	if d.test(fp) {
		t.Fatal("unmarked key tested present")
	}
	d.mark(fp)
	if !d.test(fp) {
		t.Fatal("marked key tested absent")
	}
	if d.test(0xdead_beef_cafe) {
		t.Fatal("unrelated key tested present at one mark")
	}
}

// TestColdDoorRotates pins the two-generation rotation: the current half flips
// and the mark counter resets when a half fills, and a key marked before the
// rotation survives it (its bits are in the other, untouched half).
func TestColdDoorRotates(t *testing.T) {
	d := newColdDoor(1 << 12)
	const key = uint64(7)
	d.mark(key)
	if d.cur != 0 {
		t.Fatalf("started on half %d, want 0", d.cur)
	}
	// The mark above already counts as one, so window-1 more reach the rotation
	// threshold exactly.
	for i := uint64(0); i < d.window-1; i++ {
		d.mark(1_000_000 + i)
	}
	if d.cur != 1 {
		t.Fatalf("did not rotate to half 1: cur=%d", d.cur)
	}
	if d.marks != 0 {
		t.Fatalf("marks not reset after rotation: %d", d.marks)
	}
	// The key marked before the rotation is still a member: the rotation zeroed
	// the other half, not the one holding it.
	if !d.test(key) {
		t.Fatal("key dropped after a single rotation")
	}
}

// TestColdDoorReset clears both generations.
func TestColdDoorReset(t *testing.T) {
	d := newColdDoor(1 << 12)
	d.mark(42)
	d.cur = 1
	d.marks = 5
	d.reset()
	if d.cur != 0 || d.marks != 0 {
		t.Fatalf("reset left cur=%d marks=%d", d.cur, d.marks)
	}
	if d.test(42) {
		t.Fatal("reset did not clear the marked key")
	}
}

// coldFixture fills a small-key flood past the cap and drives the migrator to
// the low-water, returning the store and a key that landed cold with its index.
func coldFixture(t *testing.T) (*Store, []byte, int) {
	t.Helper()
	const cap = 1 << 20
	s := migratorStore(t, cap)
	const n = 40000
	fillSmall(t, s, n)
	for i := 0; i < 64; i++ {
		if s.MigrateCold() == 0 {
			s.CompactArena()
			break
		}
		s.CompactArena()
	}
	if s.Cold().Records == 0 {
		t.Fatal("nothing went cold; fixture too small")
	}
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		if s.slotIsCold(k) {
			return s, k, i
		}
	}
	t.Fatal("no cold key found")
	return nil, nil, 0
}

// TestColdReadDoorkeeperCopyPromotes is the doorkeeper over GetString: the first
// cold read serves the frame and leaves the key cold, the second promotes it
// back to the arena, and both return the value.
func TestColdReadDoorkeeperCopyPromotes(t *testing.T) {
	s, ck, ci := coldFixture(t)
	var dst []byte

	dst, ok := s.GetString(ck, 0, dst)
	if !ok || string(dst) != wantVal(ci) {
		t.Fatalf("first read = %q,%v, want %q", dst, ok, wantVal(ci))
	}
	if !s.slotIsCold(ck) {
		t.Fatal("first cold read promoted the key")
	}
	before := s.Cold().Records

	dst, ok = s.GetString(ck, 0, dst)
	if !ok || string(dst) != wantVal(ci) {
		t.Fatalf("second read = %q,%v, want %q", dst, ok, wantVal(ci))
	}
	if s.slotIsCold(ck) {
		t.Fatal("second cold read did not promote the key")
	}
	if s.Cold().Records != before-1 {
		t.Fatalf("cold count = %d after promotion, want %d", s.Cold().Records, before-1)
	}
	// The promoted key now reads from the arena and still answers.
	dst, ok = s.GetString(ck, 0, dst)
	if !ok || string(dst) != wantVal(ci) {
		t.Fatalf("post-promotion read = %q,%v, want %q", dst, ok, wantVal(ci))
	}
	assertCensus(t, s)
}

// TestColdReadDoorkeeperViewPromotes is the same over the GetView path, the one
// GET and MGET use: a cold GET serves the frame view once and promotes on the
// second sighting, instead of the old unconditional bring-up on every read.
func TestColdReadDoorkeeperViewPromotes(t *testing.T) {
	s, ck, ci := coldFixture(t)

	v, ok := s.GetView(ck, 0)
	if !ok || string(v) != wantVal(ci) {
		t.Fatalf("first view = %q,%v, want %q", v, ok, wantVal(ci))
	}
	if !s.slotIsCold(ck) {
		t.Fatal("first cold view promoted the key")
	}

	v, ok = s.GetView(ck, 0)
	if !ok || string(v) != wantVal(ci) {
		t.Fatalf("second view = %q,%v, want %q", v, ok, wantVal(ci))
	}
	if s.slotIsCold(ck) {
		t.Fatal("second cold view did not promote the key")
	}
	assertCensus(t, s)
}

// TestColdReadDoorkeeperDistinctKeysStayCold is the pollution guard: reading many
// distinct cold keys once each promotes none of them, because each is a first
// sighting. A scan over the cold tier does not drag the whole tier resident.
func TestColdReadDoorkeeperDistinctKeysStayCold(t *testing.T) {
	s, _, _ := coldFixture(t)
	before := s.Cold().Records

	var dst []byte
	read := 0
	for i := 0; i < 40000 && read < 200; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		if !s.slotIsCold(k) {
			continue
		}
		if _, ok := s.GetString(k, 0, dst); !ok {
			t.Fatalf("cold key %d missing", i)
		}
		read++
	}
	if read == 0 {
		t.Fatal("read no cold keys")
	}
	if s.Cold().Records != before {
		t.Fatalf("a one-touch scan promoted %d keys; the doorkeeper let one-hit reads pollute", before-s.Cold().Records)
	}
	assertCensus(t, s)
}
