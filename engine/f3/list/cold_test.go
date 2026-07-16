package list

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The list cold-tier read plumbing (plan M7-slice-cold-chunk-list, PR D1). A
// demoted chunk releases its blob and directory and keeps only a cold-region
// offset; LINDEX and LRANGE must read it transparently, byte-for-byte with the
// resident form. The demote pass that flips chunks cold in production lands with
// its own slice and does not run until the trigger wires it live, so these tests
// drive the transition directly through handDemote and pin the read side alone.

// coldTestNative fills a native band with n distinct values each of width w,
// enough to span several chunks so a demote can target a true interior chunk. It
// returns the values so a read can be checked against them after a demote.
func coldTestNative(n, w int) (*native, [][]byte) {
	nt := &native{}
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprintf("v%0*d", w-1, i))
		nt.pushBack(v)
		want[i] = v
	}
	return nt, want
}

// handDemote packs the live frames of ring chunk ci into the cold region and flips
// the handle to the cold form: blob and directory released, the window
// canonicalized to lo == 0, the returned offset recorded. This is the transition
// the demote pass will make; the read plumbing under test is identical either way.
func handDemote(t *testing.T, nt *native, st *store.Store, ci int) {
	t.Helper()
	c := nt.ring.at(ci)
	var payload []byte
	for p := c.lo; p < c.hi; p++ {
		v, _ := c.frameAt(int(c.dir[p]))
		payload = appendFrame(payload, v)
	}
	count := c.count()
	var disc [8]byte
	binary.BigEndian.PutUint64(disc[:], uint64(ci))
	off, ok := st.AppendChunk(kindList, 0, uint16(count), []byte("k"), disc[:], payload)
	if !ok {
		t.Fatal("AppendChunk refused on a cold-configured store")
	}
	if nt.cold == nil {
		nt.cold = &listCold{st: st}
	}
	c.blob = nil
	c.dir = nil
	c.lo, c.hi = 0, count
	c.coldOff = off
	nt.coldN++
}

// coldSpan returns the dense start index and element count of ring chunk ci, so a
// window can be aimed to start inside the demoted chunk.
func coldSpan(nt *native, ci int) (start, count int) {
	for i := 0; i < ci; i++ {
		start += nt.ring.at(i).count()
	}
	return start, nt.ring.at(ci).count()
}

// TestColdChunkReadsMatchResident demotes an interior chunk and holds LINDEX and
// LRANGE to the pre-demote values: every index still reads, a full range crossing
// the cold chunk reproduces the list verbatim, and a window that starts inside the
// cold chunk exercises the ordinal skip on the pread payload.
func TestColdChunkReadsMatchResident(t *testing.T) {
	cx, _ := coldCtx(t)
	nt, want := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks for an interior demote, got %d", nt.ring.n)
	}

	ci := nt.ring.n / 2 // a true interior chunk, neither head nor tail
	handDemote(t, nt, cx.St, ci)
	if !nt.ring.at(ci).cold() {
		t.Fatal("chunk did not flip to the cold form")
	}

	// LINDEX over every position, resident chunks and the demoted one alike.
	for i := range want {
		if got := nt.at(i); !bytes.Equal(got, want[i]) {
			t.Fatalf("at(%d) = %q, want %q", i, got, want[i])
		}
	}

	// Full LRANGE: the cursor walk crosses the cold chunk and the RESP stream must
	// equal the one built from the original values.
	got := nt.rangeInto(nil, 0, nt.count-1)
	var full []byte
	for _, v := range want {
		full = resp.AppendBulk(full, v)
	}
	if !bytes.Equal(got, full) {
		t.Fatal("full rangeInto over a cold chunk != RESP of the original values")
	}

	// A window that starts one element into the cold chunk and runs past its end,
	// so the walk skips ordinals inside the pread payload and then crosses out.
	cstart, ccount := coldSpan(nt, ci)
	lo, hi := cstart+1, cstart+ccount+2
	if hi >= nt.count {
		hi = nt.count - 1
	}
	sub := nt.rangeInto(nil, lo, hi)
	var wantSub []byte
	for i := lo; i <= hi; i++ {
		wantSub = resp.AppendBulk(wantSub, want[i])
	}
	if !bytes.Equal(sub, wantSub) {
		t.Fatalf("sub-range [%d,%d] starting inside the cold chunk != original", lo, hi)
	}
}

// TestDemotePassShedsInterior drives the real demote pass end to end through the
// registry wrapper: it sheds every interior chunk in one quantum, keeps the head and
// tail chunks resident, drops the footprint, reconciles the running total, and leaves
// every element readable in order through the cold-aware reads. A second pass finds
// the interior already cold and sheds nothing more.
func TestDemotePassShedsInterior(t *testing.T) {
	cx, g := coldCtx(t)
	nt, want := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks for an interior demote, got %d", nt.ring.n)
	}
	// Hang the native band off a registry-backed list so the demote runs the whole
	// wrapper path (lookup and note reconciliation), the way the trigger will call it.
	l := &list{nat: nt, everLarge: true}
	g.m["k"] = l
	g.note(l)
	head, tail := nt.ring.front(), nt.ring.tail()
	interior := nt.ring.n - 2*demoteMargin

	before := nt.residentBytes()
	n := g.demote(cx, []byte("k"))
	if n != interior {
		t.Fatalf("demoted %d chunks, want the %d interior", n, interior)
	}
	if nt.coldN != n {
		t.Fatalf("coldN %d, want %d", nt.coldN, n)
	}
	// note reconciled the freed bytes: the running total is the shrunk footprint.
	if g.resident != nt.residentBytes() {
		t.Fatalf("running total %d != post-demote footprint %d", g.resident, nt.residentBytes())
	}

	// The margin chunks stay resident; every interior chunk sheds its blob.
	if head.cold() || tail.cold() {
		t.Fatal("a margin chunk was demoted")
	}
	for i := demoteMargin; i < nt.ring.n-demoteMargin; i++ {
		if !nt.ring.at(i).cold() {
			t.Fatalf("interior chunk %d stayed resident", i)
		}
	}

	// The shed blobs dwarf the small demote directory, so the footprint falls.
	if after := nt.residentBytes(); after >= before {
		t.Fatalf("footprint %d did not fall below %d after demote", after, before)
	}

	// LLEN is unchanged and every element still reads in order across the cold gap.
	if nt.count != len(want) {
		t.Fatalf("count %d != %d after demote", nt.count, len(want))
	}
	for i := range want {
		if got := nt.at(i); !bytes.Equal(got, want[i]) {
			t.Fatalf("at(%d) = %q, want %q", i, got, want[i])
		}
	}
	got := nt.rangeInto(nil, 0, nt.count-1)
	var full []byte
	for _, v := range want {
		full = resp.AppendBulk(full, v)
	}
	if !bytes.Equal(got, full) {
		t.Fatal("full LRANGE over the demoted interior != RESP of the original values")
	}

	// The demote descriptor count matches the chunks shed, one per demote sequence.
	if nt.cold.dir.Len() != n {
		t.Fatalf("cold directory holds %d descriptors, want %d", nt.cold.dir.Len(), n)
	}

	// A second pass finds the interior already cold and sheds nothing more.
	if again := g.demote(cx, []byte("k")); again != 0 {
		t.Fatalf("second demote shed %d, want 0 (interior already cold)", again)
	}
}

// TestColdChunkResidentBytesDrops pins the accounting win: demoting one standard
// interior chunk releases its blob and directory, so the native footprint falls by
// exactly one chunkFootprint and nothing else moves.
func TestColdChunkResidentBytesDrops(t *testing.T) {
	cx, _ := coldCtx(t)
	nt, _ := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks, got %d", nt.ring.n)
	}

	before := nt.residentBytes()
	handDemote(t, nt, cx.St, nt.ring.n/2)
	after := nt.residentBytes()

	if before-after != chunkFootprint {
		t.Fatalf("footprint fell by %d, want one chunk (%d)", before-after, chunkFootprint)
	}
}
