package obs1

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func sampleManifest() Manifest {
	return Manifest{
		Group:   2,
		Epoch:   7,
		ManSeq:  3,
		FoldPos: ChainPos{DD: 1, Seq: 900},
		FoldSeq: 8_800,
		Segs: []ManifestSeg{
			{SegSeq: 10, Level: 0, TTLClass: 0, Size: 4096, NRecords: 53, RawBytes: 1300, FooterOff: 3900, FooterLen: 180},
			{SegSeq: 12, Level: 1, TTLClass: 2, Size: 1 << 20, NRecords: 4000, RawBytes: 900_000, MinExpMS: 5_000, MaxExpMS: 9_000, DeadFrac: 250, FooterOff: 1<<20 - 600, FooterLen: 584},
		},
	}
}

func mustManifest(t *testing.T, writer uint64, m Manifest) []byte {
	t.Helper()
	b, err := AppendManifest(nil, writer, m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestChainPos(t *testing.T) {
	// Packed numeric order must equal the chain's (DD, Seq) total order,
	// which is the whole point of the one-u64 form.
	ordered := []ChainPos{
		{DD: 0, Seq: 0},
		{DD: 0, Seq: 1},
		{DD: 0, Seq: chainSeqMax},
		{DD: 1, Seq: 0},
		{DD: 1, Seq: 900},
		{DD: 255, Seq: chainSeqMax},
	}
	for i, p := range ordered {
		v, err := p.Pack()
		if err != nil {
			t.Fatalf("pack %+v: %v", p, err)
		}
		if got := UnpackChainPos(v); got != p {
			t.Fatalf("round trip %+v -> %d -> %+v", p, v, got)
		}
		for j := i + 1; j < len(ordered); j++ {
			q := ordered[j]
			w, _ := q.Pack()
			if !p.Before(q) || q.Before(p) {
				t.Fatalf("Before disagrees on %+v vs %+v", p, q)
			}
			if v >= w {
				t.Fatalf("packed order disagrees on %+v (%d) vs %+v (%d)", p, v, q, w)
			}
		}
	}
	if _, err := (ChainPos{Seq: chainSeqMax + 1}).Pack(); err == nil {
		t.Error("seq past the 56-bit ceiling packed")
	}
}

func TestManifestRoundTrip(t *testing.T) {
	want := sampleManifest()
	b := mustManifest(t, 9, want)
	if len(b) != HeaderSize+manFixed+2*manSeg+4 {
		t.Fatalf("encoded length %d", len(b))
	}
	got, h, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Writer != 9 || got.Group != want.Group || got.Epoch != want.Epoch || got.ManSeq != want.ManSeq || got.FoldPos != want.FoldPos || got.FoldSeq != want.FoldSeq {
		t.Fatalf("parsed %+v header %+v", got, h)
	}
	if len(got.Segs) != 2 || got.Segs[0] != want.Segs[0] || got.Segs[1] != want.Segs[1] {
		t.Fatalf("parsed segs %+v", got.Segs)
	}
	again := mustManifest(t, h.Writer, got)
	if !bytes.Equal(again, b) {
		t.Fatal("re-encode differs")
	}

	// A fresh group's first manifest has no segments at all.
	empty := Manifest{Group: 0, Epoch: 1, ManSeq: 0}
	eb := mustManifest(t, 1, empty)
	got, _, err = ParseManifest(eb)
	if err != nil {
		t.Fatal(err)
	}
	if got.Segs != nil || got.Group != empty.Group || got.Epoch != empty.Epoch || got.ManSeq != empty.ManSeq || got.FoldPos != empty.FoldPos {
		t.Fatalf("zero-seg parse %+v", got)
	}

	if k := manifestKey("db/a", 2, 3); k != "db/a/man/g002/0000000000000003" {
		t.Fatalf("manifestKey %q", k)
	}
}

func TestManifestWriterRejects(t *testing.T) {
	seg := func(mut func(*ManifestSeg)) Manifest {
		m := sampleManifest()
		mut(&m.Segs[1])
		return m
	}
	cases := map[string]Manifest{
		"equal seg seq":             seg(func(s *ManifestSeg) { s.SegSeq = 10 }),
		"descending seg seq":        seg(func(s *ManifestSeg) { s.SegSeq = 9 }),
		"level 2":                   seg(func(s *ManifestSeg) { s.Level = 2 }),
		"ttl class 0 with min":      seg(func(s *ManifestSeg) { s.TTLClass = 0; s.MinExpMS = 1; s.MaxExpMS = 0 }),
		"ttl class 0 with max":      seg(func(s *ManifestSeg) { s.TTLClass = 0; s.MinExpMS = 0; s.MaxExpMS = 1 }),
		"ttl class n with zero min": seg(func(s *ManifestSeg) { s.MinExpMS = 0 }),
		"ttl bounds inverted":       seg(func(s *ManifestSeg) { s.MinExpMS = 9_001 }),
		"deadfrac past per-mille":   seg(func(s *ManifestSeg) { s.DeadFrac = 1001 }),
		"footer past size":          seg(func(s *ManifestSeg) { s.FooterOff = s.Size - 100 }),
		"sized row without footer":  seg(func(s *ManifestSeg) { s.FooterLen = 0 }),
	}
	over := sampleManifest()
	over.FoldPos.Seq = chainSeqMax + 1
	cases["fold pos past ceiling"] = over
	for name, m := range cases {
		if _, err := AppendManifest(nil, 1, m); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// reCRC restores the payload crc after a deliberate body mutation, so the
// test reaches the shape check behind it instead of the crc.
func reCRC(b []byte) {
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32c(b[HeaderSize:len(b)-4]))
}

func TestManifestParseRejects(t *testing.T) {
	good := mustManifest(t, 9, sampleManifest())

	flip := func(name string, i int) {
		t.Helper()
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := ParseManifest(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	flip("group byte", HeaderSize)
	flip("fold pos byte", HeaderSize+14)
	flip("fold seq byte", HeaderSize+22)
	flip("seg row byte", HeaderSize+manFixed+2)
	flip("crc byte", len(good)-1)

	if _, _, err := ParseManifest(good[:len(good)-1]); err == nil {
		t.Error("truncated manifest accepted")
	}
	if _, _, err := ParseManifest(good[:HeaderSize+4]); err == nil {
		t.Error("headerless stub accepted")
	}
	badVersion := AppendHeader(nil, Header{Format: FormatManifest, FVersion: 2, Writer: 9})
	badVersion = append(badVersion, good[HeaderSize:]...)
	if _, _, err := ParseManifest(badVersion); err == nil {
		t.Error("fversion 2 accepted")
	}
	crossType := AppendHeader(nil, Header{Format: FormatWAL, FVersion: 1, Writer: 9})
	crossType = append(crossType, good[HeaderSize:]...)
	if _, _, err := ParseManifest(crossType); err == nil {
		t.Error("cross-typed object accepted")
	}

	// Valid crc, rows out of order: only the canonical-form check can say no.
	swapped := append([]byte(nil), good...)
	r0 := HeaderSize + manFixed
	row := append([]byte(nil), swapped[r0:r0+manSeg]...)
	copy(swapped[r0:], swapped[r0+manSeg:r0+2*manSeg])
	copy(swapped[r0+manSeg:], row)
	reCRC(swapped)
	if _, _, err := ParseManifest(swapped); err == nil {
		t.Error("out-of-order rows with valid crc accepted")
	}

	// Valid crc, nsegs disagreeing with the payload length.
	counted := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(counted[HeaderSize+30:], 3)
	reCRC(counted)
	if _, _, err := ParseManifest(counted); err == nil {
		t.Error("nsegs vs payload length mismatch accepted")
	}
}

// fakeHist maps each epoch to the last chain position at which it was the
// group's current lease epoch. The lease fold slice will answer this from
// the chain itself; the manifest reader only needs the answer.
type fakeHist map[uint32]ChainPos

func (h fakeHist) EpochCurrentAtOrAfter(_ uint16, epoch uint32, from ChainPos) bool {
	last, ok := h[epoch]
	return ok && !last.Before(from)
}

func TestSelectManifest(t *testing.T) {
	man := func(group uint16, seq uint64, epoch uint32, fold uint64) Manifest {
		return Manifest{Group: group, Epoch: epoch, ManSeq: seq, FoldPos: ChainPos{Seq: fold}}
	}
	// Epoch 3 held the lease through chain seq 100, epoch 4 ever since.
	hist := fakeHist{
		3: {Seq: 100},
		4: {Seq: chainSeqMax},
	}

	if _, ok := SelectManifest(2, nil, hist); ok {
		t.Error("empty list produced a manifest")
	}
	if _, ok := SelectManifest(2, []Manifest{man(2, 0, 99, 10)}, hist); ok {
		t.Error("unknown epoch produced a manifest")
	}

	// The zombie: epoch 3's folder writes seq 2 after epoch 4 already
	// folded past chain seq 100. Its epoch is real history, so a cursorless
	// check would accept it; the fold cursor is what rules it out.
	zombie := []Manifest{man(2, 0, 3, 50), man(2, 1, 4, 120), man(2, 2, 3, 60)}
	if !hist.EpochCurrentAtOrAfter(2, 3, ChainPos{}) {
		t.Fatal("test premise broken: epoch 3 should look fine from a zero cursor")
	}
	got, ok := SelectManifest(2, zombie, hist)
	if !ok || got.ManSeq != 1 {
		t.Fatalf("zombie scenario picked %+v ok=%v, want seq 1", got, ok)
	}

	// A wrong-group entry is ignored entirely, including for the ManSeq
	// ordering guard: group 5's seq 9 must not shadow group 2's seq 0.
	mixed := []Manifest{man(5, 9, 4, 10), man(2, 0, 4, 10)}
	got, ok = SelectManifest(2, mixed, hist)
	if !ok || got.ManSeq != 0 {
		t.Fatalf("mixed-group scenario picked %+v ok=%v, want seq 0", got, ok)
	}

	// Duplicate and regressing seqs are data errors, skipped not applied.
	dup := []Manifest{man(2, 3, 4, 10), man(2, 3, 4, 999), man(2, 2, 4, 999)}
	got, ok = SelectManifest(2, dup, hist)
	if !ok || got.FoldPos.Seq != 10 {
		t.Fatalf("duplicate-seq scenario picked %+v ok=%v, want fold 10", got, ok)
	}
}

func FuzzParseManifest(f *testing.F) {
	good, err := AppendManifest(nil, 9, sampleManifest())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	empty, err := AppendManifest(nil, 1, Manifest{Group: 1, Epoch: 1})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(empty)
	f.Fuzz(func(t *testing.T, b []byte) {
		m, h, err := ParseManifest(b)
		if err != nil {
			return
		}
		again, err := AppendManifest(nil, h.Writer, m)
		if err != nil {
			t.Fatalf("accepted manifest fails re-encode: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatal("accepted manifest re-encodes differently")
		}
	})
}
