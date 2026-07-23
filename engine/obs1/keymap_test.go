package obs1

import (
	"math/rand/v2"
	"testing"
)

func TestKeymapPutLookupDelete(t *testing.T) {
	m := NewKeymap()
	if _, ok := m.Lookup(7); ok {
		t.Fatal("empty map found something")
	}
	if err := m.Put(7, KeyLoc{Seg: 3, Chunk: 12, Tier: 1}); err != nil {
		t.Fatal(err)
	}
	got, ok := m.Lookup(7)
	if !ok || got != (KeyLoc{Seg: 3, Chunk: 12, Tier: 1}) {
		t.Fatalf("lookup got %+v ok=%v", got, ok)
	}
	if err := m.Put(7, KeyLoc{Seg: 9, Chunk: 1}); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Lookup(7); got != (KeyLoc{Seg: 9, Chunk: 1}) {
		t.Fatalf("overwrite got %+v", got)
	}
	if m.Len() != 1 {
		t.Fatalf("Len=%d after overwrite", m.Len())
	}
	if !m.Delete(7) {
		t.Fatal("delete of a live entry said absent")
	}
	if m.Delete(7) {
		t.Fatal("second delete said present")
	}
	if _, ok := m.Lookup(7); ok {
		t.Fatal("deleted key still resolves")
	}
	if m.Len() != 0 {
		t.Fatalf("Len=%d after delete", m.Len())
	}
}

func TestKeymapPackBounds(t *testing.T) {
	m := NewKeymap()
	if err := m.Put(1, KeyLoc{Seg: 0}); err == nil {
		t.Fatal("SegSeq zero accepted")
	}
	if err := m.Put(1, KeyLoc{Seg: 1, Chunk: 1 << 24}); err == nil {
		t.Fatal("25-bit chunk accepted")
	}
	if err := m.Put(1, KeyLoc{Seg: 1, Tier: 4}); err == nil {
		t.Fatal("3-bit tier accepted")
	}
	if err := m.Shadow(1, KeyLoc{Seg: 0}, false); err == nil {
		t.Fatal("Shadow accepted SegSeq zero")
	}
	if err := m.Put(1, KeyLoc{Seg: 1, Chunk: 1<<24 - 1, Tier: 3}); err != nil {
		t.Fatalf("max in-range locator refused: %v", err)
	}
	if got, _ := m.Lookup(1); got != (KeyLoc{Seg: 1, Chunk: 1<<24 - 1, Tier: 3}) {
		t.Fatalf("max locator round-trip got %+v", got)
	}
}

func TestKeymapZeroFingerprint(t *testing.T) {
	m := NewKeymap()
	if err := m.Put(0, KeyLoc{Seg: 5, Chunk: 2}); err != nil {
		t.Fatal(err)
	}
	got, ok := m.Lookup(0)
	if !ok || got.Seg != 5 {
		t.Fatalf("fp 0 got %+v ok=%v", got, ok)
	}
	if !m.Delete(0) {
		t.Fatal("fp 0 delete said absent")
	}
}

// TestKeymapChurnModel drives the table against a reference map through
// random puts, deletes, and lookups, across several growth doublings,
// then sweeps both directions.
func TestKeymapChurnModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(2064, 23))
	m := NewKeymap()
	ref := make(map[uint64]KeyLoc)
	// Small fingerprint space forces overwrites; deletes force backward
	// shifts inside dense clusters.
	fpOf := func() uint64 { return rng.Uint64N(20000) }
	for i := range 200000 {
		fp := fpOf()
		switch rng.IntN(3) {
		case 0, 1:
			l := KeyLoc{Seg: rng.Uint32N(1000) + 1, Chunk: rng.Uint32N(1 << 24), Tier: uint8(rng.UintN(4))}
			if err := m.Put(fp, l); err != nil {
				t.Fatal(err)
			}
			ref[fp] = l
		case 2:
			got := m.Delete(fp)
			_, want := ref[fp]
			if got != want {
				t.Fatalf("op %d: delete(%d)=%v want %v", i, fp, got, want)
			}
			delete(ref, fp)
		}
		if i%1000 == 0 {
			probe := fpOf()
			got, ok := m.Lookup(probe)
			want, wok := ref[probe]
			if ok != wok || got != want {
				t.Fatalf("op %d: lookup(%d)=%+v,%v want %+v,%v", i, probe, got, ok, want, wok)
			}
		}
	}
	if m.Len() != len(ref) {
		t.Fatalf("Len=%d ref=%d", m.Len(), len(ref))
	}
	for fp, want := range ref {
		got, ok := m.Lookup(fp)
		if !ok || got != want {
			t.Fatalf("sweep: lookup(%d)=%+v,%v want %+v", fp, got, ok, want)
		}
	}
}

func TestKeymapGrowth(t *testing.T) {
	m := NewKeymap()
	before := m.Bytes()
	n := 100000
	for i := 1; i <= n; i++ {
		if err := m.Put(uint64(i)*0x9E3779B97F4A7C15, KeyLoc{Seg: uint32(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if m.Len() != n {
		t.Fatalf("Len=%d want %d", m.Len(), n)
	}
	if m.Bytes() <= before {
		t.Fatal("Bytes did not grow")
	}
	if slots := m.Bytes() / 16; m.Len()*keymapLoadDen > slots*keymapLoadNum {
		t.Fatalf("load over the cap: %d entries in %d slots", m.Len(), slots)
	}
	for i := 1; i <= n; i++ {
		got, ok := m.Lookup(uint64(i) * 0x9E3779B97F4A7C15)
		if !ok || got.Seg != uint32(i) {
			t.Fatalf("post-growth lookup %d got %+v ok=%v", i, got, ok)
		}
	}
}

func TestKeymapShadowRebuild(t *testing.T) {
	m := NewKeymap()
	// Claims arrive out of segment order; the highest SegSeq must win
	// whether it lands first or last.
	if err := m.Shadow(1, KeyLoc{Seg: 5, Chunk: 50}, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Shadow(1, KeyLoc{Seg: 3, Chunk: 30}, false); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Lookup(1); got.Seg != 5 {
		t.Fatalf("lower seg displaced higher: %+v", got)
	}
	if err := m.Shadow(1, KeyLoc{Seg: 8, Chunk: 80}, false); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Lookup(1); got.Seg != 8 || got.Chunk != 80 {
		t.Fatalf("higher seg did not displace: %+v", got)
	}

	// A tombstone claim from a higher segment shadows the key: reads say
	// absent, and a lower-seg value arriving later cannot resurrect it.
	if err := m.Shadow(2, KeyLoc{Seg: 9}, true); err != nil {
		t.Fatal(err)
	}
	if err := m.Shadow(2, KeyLoc{Seg: 4, Chunk: 40}, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Lookup(2); ok {
		t.Fatal("tombstone-shadowed key resolves")
	}
	// A higher value over a lower tombstone wins.
	if err := m.Shadow(3, KeyLoc{Seg: 2}, true); err != nil {
		t.Fatal(err)
	}
	if err := m.Shadow(3, KeyLoc{Seg: 6, Chunk: 60}, false); err != nil {
		t.Fatal(err)
	}
	if got, ok := m.Lookup(3); !ok || got.Seg != 6 {
		t.Fatalf("value over lower tombstone got %+v ok=%v", got, ok)
	}

	if got := m.FinishRebuild(); got != 1 {
		t.Fatalf("FinishRebuild removed %d want 1", got)
	}
	if _, ok := m.Lookup(2); ok {
		t.Fatal("swept key resolves")
	}
	if got, ok := m.Lookup(1); !ok || got.Seg != 8 {
		t.Fatalf("survivor 1 got %+v ok=%v", got, ok)
	}
	if got, ok := m.Lookup(3); !ok || got.Seg != 6 {
		t.Fatalf("survivor 3 got %+v ok=%v", got, ok)
	}
	if m.Len() != 2 {
		t.Fatalf("Len=%d after sweep", m.Len())
	}
}

// TestKeymapShadowSweepModel churns Shadow claims over a fingerprint
// space sized to force dense clusters and wraparound, then checks the
// sweep leaves exactly the keys whose winning claim was a value.
func TestKeymapShadowSweepModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(858, 23))
	m := NewKeymap()
	type claim struct {
		seg  uint32
		loc  KeyLoc
		dead bool
	}
	ref := make(map[uint64]claim)
	for range 50000 {
		fp := rng.Uint64N(5000)
		seg := rng.Uint32N(500) + 1
		dead := rng.IntN(3) == 0
		l := KeyLoc{Seg: seg, Chunk: rng.Uint32N(1 << 20)}
		if err := m.Shadow(fp, l, dead); err != nil {
			t.Fatal(err)
		}
		if cur, ok := ref[fp]; !ok || seg > cur.seg {
			ref[fp] = claim{seg: seg, loc: l, dead: dead}
		}
	}
	dead := 0
	for _, c := range ref {
		if c.dead {
			dead++
		}
	}
	if got := m.FinishRebuild(); got != dead {
		t.Fatalf("swept %d want %d", got, dead)
	}
	live := 0
	for fp, c := range ref {
		got, ok := m.Lookup(fp)
		if c.dead {
			if ok {
				t.Fatalf("dead fp %d resolves to %+v", fp, got)
			}
			continue
		}
		live++
		if !ok || got != c.loc {
			t.Fatalf("live fp %d got %+v ok=%v want %+v", fp, got, ok, c.loc)
		}
	}
	if m.Len() != live {
		t.Fatalf("Len=%d live=%d", m.Len(), live)
	}
}
