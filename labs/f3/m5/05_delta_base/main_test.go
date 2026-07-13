package main

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

// TestEncodersMatchStdlib pins the lab's byte pricing to the standard library's
// varint writers, the same ones engine/f3/stream/id.go uses. If uvlen or vlen
// ever drifts from binary.AppendUvarint / binary.AppendVarint the lab's byte
// columns would lie, so guard the pricing directly.
func TestEncodersMatchStdlib(t *testing.T) {
	vals := []uint64{0, 1, 127, 128, 300, 16383, 16384, 1 << 20, 1 << 35, ^uint64(0)}
	for _, v := range vals {
		if got, want := uvlen(v), len(binary.AppendUvarint(nil, v)); got != want {
			t.Fatalf("uvlen(%d) = %d, stdlib = %d", v, got, want)
		}
	}
	svals := []int64{0, 1, -1, 63, -64, 64, -65, 1 << 20, -(1 << 20), 1 << 34}
	for _, v := range svals {
		if got, want := vlen(v), len(binary.AppendVarint(nil, v)); got != want {
			t.Fatalf("vlen(%d) = %d, stdlib = %d", v, got, want)
		}
	}
}

// TestRoundTrip proves both bases decode back to the original IDs across every
// pattern: a codec that saved bytes but lost an ID would be worthless. It walks
// the real byte streams the size sweep prices.
func TestRoundTrip(t *testing.T) {
	for _, p := range patterns() {
		rng := rand.New(rand.NewSource(1))
		ids := p.gen(128, rng)
		first := ids[0]
		want := ids[len(ids)-1]
		if got := decodeBase(encodeBase(ids), first); got != want {
			t.Fatalf("%s base round-trip: last = %+v, want %+v", p.name, got, want)
		}
		if got := decodeSucc(encodeSucc(ids), first); got != want {
			t.Fatalf("%s succ round-trip: last = %+v, want %+v", p.name, got, want)
		}
		// Walk every position, not just the last, so a mid-stream corruption is caught.
		checkAll(t, p.name, ids, first)
	}
}

// checkAll decodes each ID at its own offset under both bases and compares to
// the source. For base-delta this exercises the independent-decode property the
// spec cites; for successive-delta it confirms the accumulated walk stays exact.
func checkAll(t *testing.T, name string, ids []id, first id) {
	t.Helper()
	// base: every entry is decodable from its own offset against firstID.
	bb := encodeBase(ids)
	pos := 0
	for i := range ids {
		md, n1 := binary.Uvarint(bb[pos:])
		sd, n2 := binary.Varint(bb[pos+n1:])
		got := id{ms: first.ms + md, seq: uint64(int64(first.seq) + sd)}
		if got != ids[i] {
			t.Fatalf("%s base entry %d = %+v, want %+v", name, i, got, ids[i])
		}
		pos += n1 + n2
	}
	// succ: entries reconstruct only in order, from the running predecessor.
	sb := encodeSucc(ids)
	pos, prev := 0, first
	for i := range ids {
		md, n1 := binary.Uvarint(sb[pos:])
		sd, n2 := binary.Varint(sb[pos+n1:])
		cur := id{ms: prev.ms + md, seq: uint64(int64(prev.seq) + sd)}
		if cur != ids[i] {
			t.Fatalf("%s succ entry %d = %+v, want %+v", name, i, cur, ids[i])
		}
		prev = cur
		pos += n1 + n2
	}
}

// TestSuccessiveNeverLarger is the lab's headline claim as an invariant: over a
// full block, successive-delta never costs more ID bytes than base-delta. The ms
// field proves it structurally (the gap to the predecessor is never wider than
// the gap to firstID, and uvarint length is monotone), and the seq field holds
// empirically across every representative pattern; asserting the block total
// guards the verdict against a codec edit that would erode it.
func TestSuccessiveNeverLarger(t *testing.T) {
	for _, p := range patterns() {
		rng := rand.New(rand.NewSource(1))
		ids := p.gen(128, rng)
		if sb, bb := succBytes(ids), baseBytes(ids); sb > bb {
			t.Fatalf("%s: successive %d B > base %d B; the byte win does not hold", p.name, sb, bb)
		}
	}
}
