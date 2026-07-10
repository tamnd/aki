package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"
)

// aliasesArena reports whether v's backing array lies inside the store's
// arena buffer, i.e. the read was a view, not a copy.
func aliasesArena(s *Store, v []byte) bool {
	if len(v) == 0 {
		return false
	}
	p := uintptr(unsafe.Pointer(&v[0]))
	base := uintptr(unsafe.Pointer(&s.arena.buf[0]))
	return p >= base && p < base+uintptr(len(s.arena.buf))
}

// TestGetViewEmbeddedAliasesArena pins the point of the view path: an
// embedded value comes back as arena bytes, not a copy, and matches what
// GetString reads.
func TestGetViewEmbeddedAliasesArena(t *testing.T) {
	s := newTestStore()
	val := bytes.Repeat([]byte("e"), 512)
	if err := s.SetString([]byte("k"), val, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, ok := s.GetView([]byte("k"), 0)
	if !ok || !bytes.Equal(v, val) {
		t.Fatalf("GetView = %d bytes, %v; want the 512B value", len(v), ok)
	}
	if !aliasesArena(s, v) {
		t.Fatal("embedded GetView copied; want a view into the arena")
	}
}

// TestGetViewSepRunAliasesArena is the same pin for the separated band while
// the run is arena-resident.
func TestGetViewSepRunAliasesArena(t *testing.T) {
	s := newTestStore()
	val := bytes.Repeat([]byte("s"), 4096)
	if err := s.SetString([]byte("k"), val, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, ok := s.GetView([]byte("k"), 0)
	if !ok || !bytes.Equal(v, val) {
		t.Fatalf("GetView = %d bytes, %v; want the 4KiB value", len(v), ok)
	}
	if !aliasesArena(s, v) {
		t.Fatal("separated-run GetView copied; want a view into the arena")
	}
}

// TestGetViewIntRendersCopy pins the copying bands: an int cell has no text
// in the arena to view, so the render must not alias it.
func TestGetViewIntRendersCopy(t *testing.T) {
	s := newTestStore()
	if err := s.SetString([]byte("n"), []byte("12345"), 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, ok := s.GetView([]byte("n"), 0)
	if !ok || string(v) != "12345" {
		t.Fatalf("GetView = %q, %v; want 12345", v, ok)
	}
	if aliasesArena(s, v) {
		t.Fatal("int GetView aliases the arena; the cell is binary, not text")
	}
}

// TestGetViewMissAndAgreement runs GetView against GetString over every band
// a point read serves, plus the miss.
func TestGetViewMissAndAgreement(t *testing.T) {
	s := newTestStore()
	if v, ok := s.GetView([]byte("missing"), 0); ok || len(v) != 0 {
		t.Fatalf("GetView(missing) = %q, %v; want empty, false", v, ok)
	}
	vals := map[string][]byte{
		"int":   []byte("-42"),
		"small": []byte("hi"),
		"emb":   bytes.Repeat([]byte("a"), 1024),
		"sep":   bytes.Repeat([]byte("b"), 1025),
		"run":   bytes.Repeat([]byte("c"), 32<<10),
	}
	for k, val := range vals {
		if err := s.SetString([]byte(k), val, 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	for k, val := range vals {
		v, ok := s.GetView([]byte(k), 0)
		if !ok || !bytes.Equal(v, val) {
			t.Fatalf("GetView(%s) = %d bytes, %v; want %d bytes", k, len(v), ok, len(val))
		}
		g, ok := s.GetString([]byte(k), 0, nil)
		if !ok || !bytes.Equal(g, val) {
			t.Fatalf("GetString(%s) disagrees", k)
		}
	}
}

// TestGetViewStreamBands checks the GetStream split carries over: chunked
// values stream, resident values view.
func TestGetViewStreamBands(t *testing.T) {
	s := New(16<<20, 1<<20)
	big := bytes.Repeat([]byte("g"), strChunkMin)
	if err := s.SetString([]byte("big"), big, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, cs, ok := s.GetViewStream([]byte("big"), 0)
	if !ok || cs == nil || len(v) != 0 {
		t.Fatalf("GetViewStream(big) = %d bytes, %v, %v; want a stream", len(v), cs, ok)
	}
	if cs.Total() != int64(len(big)) {
		t.Fatalf("stream total = %d, want %d", cs.Total(), len(big))
	}
	if err := s.SetString([]byte("small"), []byte("v"), 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, cs, ok = s.GetViewStream([]byte("small"), 0)
	if !ok || cs != nil || string(v) != "v" {
		t.Fatalf("GetViewStream(small) = %q, %v, %v", v, cs, ok)
	}
	if _, cs, ok := s.GetViewStream([]byte("none"), 0); ok || cs != nil {
		t.Fatal("GetViewStream(none) reported a hit")
	}
}

// TestGetViewDiesAtNextWrite documents the lifetime rule from the other side:
// an in-place overwrite of the same key mutates the bytes a still-held view
// names. Callers must consume the view before the next store call; this test
// is the reason the rule exists.
func TestGetViewDiesAtNextWrite(t *testing.T) {
	s := newTestStore()
	a := bytes.Repeat([]byte("a"), 512)
	b := bytes.Repeat([]byte("b"), 512)
	if err := s.SetString([]byte("k"), a, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	v, ok := s.GetView([]byte("k"), 0)
	if !ok || !aliasesArena(s, v) {
		t.Fatal("setup: want an arena view")
	}
	saved := append([]byte(nil), v...)
	if err := s.SetString([]byte("k"), b, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, b) {
		t.Fatal("in-place overwrite did not reuse the viewed bytes; the lifetime rule may be stale")
	}
	if !bytes.Equal(saved, a) {
		t.Fatal("copy taken before the write lost the old value")
	}
}

// BenchmarkReadValue is the copy-vs-view A/B this API exists for, one pair
// per resident band: 64B and 1KiB embedded, 4KiB separated (arena run). The
// copy lane is GetString into a reused scratch, the exact read GET ran before
// the view path.
func BenchmarkReadValue(b *testing.B) {
	for _, sz := range []int{64, 1024, 4096} {
		keys := 1 << 20
		if sz == 4096 {
			keys = 1 << 18
		}
		s := New(keys*(sz+96)+keys*(sz+96)/4+(16<<20), 0)
		val := make([]byte, sz)
		var kb [16]byte
		fill := func(i int) []byte {
			binary.LittleEndian.PutUint64(kb[0:8], uint64(i))
			binary.LittleEndian.PutUint64(kb[8:16], uint64(i)*0x9e3779b97f4a7c15)
			return kb[:16]
		}
		for i := 0; i < keys; i++ {
			if err := s.SetString(fill(i), val, 0, 0, false); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("copy/%d", sz), func(b *testing.B) {
			var dst []byte
			b.SetBytes(int64(sz))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var ok bool
				dst, ok = s.GetString(fill(i&(keys-1)), 0, dst)
				if !ok {
					b.Fatal("miss")
				}
			}
		})
		b.Run(fmt.Sprintf("view/%d", sz), func(b *testing.B) {
			b.SetBytes(int64(sz))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v, ok := s.GetView(fill(i&(keys-1)), 0)
				if !ok || len(v) != sz {
					b.Fatal("miss")
				}
			}
		})
	}
}
