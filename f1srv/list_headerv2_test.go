package f1srv

import (
	"encoding/binary"
	"testing"
)

// The header widening (impl/31) has to be behaviour-preserving on its own: a v2 header must carry
// the v1 dense fields at their original offsets so every dense reader decodes it unchanged, and it
// must round-trip the new sparse-model fields (everSparse, count, generation) so the later sparse
// slices can rely on them. These tests pin both properties before any command reads the new fields.

// v1DenseDecode mimics the inline decode listHeader and listHeaderAt do: it reads only the four v1
// fields from the front of a header record. If widening to v2 disturbed those offsets, this would
// diverge from listHeaderDecodeFull, so the test compares the two.
func v1DenseDecode(v []byte) (head, tail int64, lpBytes uint64, everLarge bool) {
	head = int64(binary.LittleEndian.Uint64(v[0:8]))
	tail = int64(binary.LittleEndian.Uint64(v[8:16]))
	lpBytes = binary.LittleEndian.Uint64(v[16:24])
	everLarge = v[24] != 0
	return
}

func TestListHeaderV2RoundTrip(t *testing.T) {
	cases := []struct {
		head, tail int64
		lpBytes    uint64
		everLarge  bool
		everSparse bool
		count      uint64
		generation uint64
	}{
		{0, 0, listHeaderBytes, false, false, 0, 0},
		{5, 12, 9001, true, false, 7, 0},
		{-3, 4, 42, false, true, 7, 3},
		{1 << 40, (1 << 40) + 100, 1 << 20, true, true, 100, 1 << 16},
		{-(1 << 40), 0, 8192, true, true, 1 << 40, 1<<63 - 1},
	}
	for i, tc := range cases {
		var ob [listMetaSizeV2]byte
		listPackHeaderV2(&ob, tc.head, tc.tail, tc.lpBytes, tc.everLarge, tc.everSparse, tc.count, tc.generation)

		head, tail, lpBytes, everLarge, everSparse, count, generation := listHeaderDecodeFull(ob[:])
		if head != tc.head || tail != tc.tail || lpBytes != tc.lpBytes || everLarge != tc.everLarge {
			t.Fatalf("case %d v1 fields: got head=%d tail=%d lp=%d large=%v, want %d %d %d %v",
				i, head, tail, lpBytes, everLarge, tc.head, tc.tail, tc.lpBytes, tc.everLarge)
		}
		if everSparse != tc.everSparse || count != tc.count || generation != tc.generation {
			t.Fatalf("case %d sparse fields: got sparse=%v count=%d gen=%d, want %v %d %d",
				i, everSparse, count, generation, tc.everSparse, tc.count, tc.generation)
		}

		// A dense reader that only looks at the front of the record must see the same four fields,
		// proving the widening left the v1 offsets untouched.
		vh, vt, vlp, vlarge := v1DenseDecode(ob[:])
		if vh != tc.head || vt != tc.tail || vlp != tc.lpBytes || vlarge != tc.everLarge {
			t.Fatalf("case %d dense reader diverged: got %d %d %d %v, want %d %d %d %v",
				i, vh, vt, vlp, vlarge, tc.head, tc.tail, tc.lpBytes, tc.everLarge)
		}
	}
}

// TestListPackHeaderDenseDefaults pins that the dense wrapper derives the sparse fields the same way
// the back-compat decode does: everSparse clear, count = tail-head, generation 0. This is what keeps
// a dense list's LLEN authority (count) equal to the window width it has always used.
func TestListPackHeaderDenseDefaults(t *testing.T) {
	var ob [listMetaSizeV2]byte
	listPackHeader(&ob, 10, 27, 555, true)
	head, tail, lpBytes, everLarge, everSparse, count, generation := listHeaderDecodeFull(ob[:])
	if head != 10 || tail != 27 || lpBytes != 555 || !everLarge {
		t.Fatalf("v1 fields: got %d %d %d %v", head, tail, lpBytes, everLarge)
	}
	if everSparse {
		t.Fatalf("dense list must not be sparse")
	}
	if count != uint64(tail-head) {
		t.Fatalf("dense count: got %d, want %d", count, tail-head)
	}
	if generation != 0 {
		t.Fatalf("dense generation: got %d, want 0", generation)
	}
}

// TestListHeaderDecodeV1BackCompat pins that a short (25-byte) record still decodes, deriving the
// sparse fields the dense way, so a header written before the widening reads back consistently.
func TestListHeaderDecodeV1BackCompat(t *testing.T) {
	var v1 [listMetaSize]byte
	binary.LittleEndian.PutUint64(v1[0:8], uint64(int64(4)))
	binary.LittleEndian.PutUint64(v1[8:16], uint64(int64(19)))
	binary.LittleEndian.PutUint64(v1[16:24], 777)
	v1[24] = 1

	head, tail, lpBytes, everLarge, everSparse, count, generation := listHeaderDecodeFull(v1[:])
	if head != 4 || tail != 19 || lpBytes != 777 || !everLarge {
		t.Fatalf("v1 fields: got %d %d %d %v", head, tail, lpBytes, everLarge)
	}
	if everSparse || generation != 0 {
		t.Fatalf("v1 record must derive sparse=false gen=0, got sparse=%v gen=%d", everSparse, generation)
	}
	if count != uint64(tail-head) {
		t.Fatalf("v1 count: got %d, want %d", count, tail-head)
	}
}
