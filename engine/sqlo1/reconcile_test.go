package sqlo1

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// The W3 helper contract: ReconcileRef recognizes exactly the roots
// whose frames W2 may elide, SegCounts reads the countable header,
// and ReconcileRoot patches count and min_expire under the
// lower-only rule. Built on real encoder output so the helpers and
// the hash layer can never drift apart silently.

func reconTestRoot(t *testing.T, count uint64, minExpMs int64) []byte {
	t.Helper()
	r := &hashSegRoot{
		rootgen:   1,
		rooth:     0xfeedbeefcafe,
		count:     count,
		nextSegid: 2,
		minExpMs:  minExpMs,
		fence:     []hashFenceEnt{{lo: 0, segid: 1, meta: hashSegMeta(int(count), minExpMs)}},
	}
	return appendHashSegRoot(nil, r)
}

func reconTestSeg(n int, minExpMs int64) []byte {
	b := make([]byte, hashSegHdrLen)
	putHashSegHdr(b, n, minExpMs)
	return b
}

func TestReconcileRef(t *testing.T) {
	root := reconTestRoot(t, 7, 0)
	rooth, ok := ReconcileRef(root)
	if !ok || rooth != 0xfeedbeefcafe {
		t.Fatalf("ReconcileRef(segmented) = %x, %v", rooth, ok)
	}
	inline := appendHashInlineHdr(nil, 1, 0)
	if _, ok := ReconcileRef(inline); ok {
		t.Fatal("ReconcileRef accepted an inline root")
	}
	if _, ok := ReconcileRef(root[:hashSegRootHdrLen-1]); ok {
		t.Fatal("ReconcileRef accepted a short payload")
	}
	if _, ok := ReconcileRef([]byte("plain string value")); ok {
		t.Fatal("ReconcileRef accepted a plain value")
	}
}

func TestSegCounts(t *testing.T) {
	n, minExp, ok := SegCounts(reconTestSeg(37, 5000))
	if !ok || n != 37 || minExp != 5000 {
		t.Fatalf("SegCounts = %d, %d, %v", n, minExp, ok)
	}
	if _, _, ok := SegCounts(make([]byte, hashSegHdrLen-1)); ok {
		t.Fatal("SegCounts accepted a short payload")
	}
	n, minExp, ok = SegCounts(reconTestSeg(0, 0))
	if !ok || n != 0 || minExp != 0 {
		t.Fatalf("SegCounts(empty seg) = %d, %d, %v", n, minExp, ok)
	}
}

func TestReconcileRootCount(t *testing.T) {
	root := reconTestRoot(t, 10, 0)
	patched, err := ReconcileRoot(root, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(patched[16:]); got != 13 {
		t.Fatalf("patched count = %d, want 13", got)
	}
	if !bytes.Equal(patched[:16], root[:16]) || !bytes.Equal(patched[24:], root[24:]) {
		t.Fatal("patch touched bytes outside the count")
	}
	if bytes.Equal(root[16:24], patched[16:24]) {
		t.Fatal("patch did not copy before writing")
	}
	down, err := ReconcileRoot(root, -9, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(down[16:]); got != 1 {
		t.Fatalf("patched count = %d, want 1", got)
	}
	if _, err := decodeHashSegRoot(patched, nil, nil); err != nil {
		t.Fatalf("patched root fails decode: %v", err)
	}
}

func TestReconcileRootMinExpire(t *testing.T) {
	// Setting a TTL where none existed: min lands and the flag flips.
	root := reconTestRoot(t, 10, 0)
	patched, err := ReconcileRoot(root, 0, 7000)
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(binary.LittleEndian.Uint64(patched[32:])); got != 7000 {
		t.Fatalf("patched min_expire = %d, want 7000", got)
	}
	if patched[1]&hflagAnyTTL == 0 {
		t.Fatal("patched root lost the TTL flag")
	}
	if _, err := decodeHashSegRoot(patched, nil, nil); err != nil {
		t.Fatalf("patched root fails decode: %v", err)
	}
	// Lowering an existing min.
	root = reconTestRoot(t, 10, 9000)
	patched, err = ReconcileRoot(root, 0, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(binary.LittleEndian.Uint64(patched[32:])); got != 4000 {
		t.Fatalf("patched min_expire = %d, want 4000", got)
	}
	// A higher post-image min never raises: stale-early is allowed,
	// stale-late is not (H-I6).
	patched, err = ReconcileRoot(root, 1, 20000)
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(binary.LittleEndian.Uint64(patched[32:])); got != 9000 {
		t.Fatalf("min_expire raised to %d, want 9000 kept", got)
	}
}

func TestReconcileRootRejects(t *testing.T) {
	root := reconTestRoot(t, 10, 0)
	if _, err := ReconcileRoot(root, -10, 0); err == nil {
		t.Fatal("ReconcileRoot allowed a count of zero")
	}
	if _, err := ReconcileRoot(root, -11, 0); err == nil {
		t.Fatal("ReconcileRoot allowed a negative count")
	}
	inline := appendHashInlineHdr(nil, 1, 0)
	if _, err := ReconcileRoot(inline, 1, 0); err == nil {
		t.Fatal("ReconcileRoot patched an inline root")
	}
	// Flipping the paged bit on a flat image reinterprets its fence as
	// a page index with a zero weight, which must fail the decode.
	paged := bytes.Clone(root)
	paged[1] |= hflagFencePaged
	if _, err := ReconcileRoot(paged, 1, 0); err == nil {
		t.Fatal("ReconcileRoot patched a corrupt paged root")
	}
}

func reconTestPagedRoot(t *testing.T, count uint64, minExpMs int64) []byte {
	t.Helper()
	r := &hashSegRoot{
		rootgen:   3,
		rooth:     0xfeedbeefcafe,
		count:     count,
		nextSegid: 131,
		minExpMs:  minExpMs,
		paged:     true,
		pidx: []hashPageEnt{
			{lo: 0, pageid: 129, weight: 40},
			{lo: 1 << 41, pageid: 130, weight: 25},
		},
	}
	return appendHashSegRoot(nil, r)
}

func TestReconcilePages(t *testing.T) {
	pageids, paged, err := ReconcilePages(reconTestPagedRoot(t, 600, 0))
	if err != nil || !paged {
		t.Fatalf("ReconcilePages(paged) = %v, %v", paged, err)
	}
	if len(pageids) != 2 || pageids[0] != 129 || pageids[1] != 130 {
		t.Fatalf("pageids = %v, want [129 130]", pageids)
	}
	if _, paged, err := ReconcilePages(reconTestRoot(t, 7, 0)); err != nil || paged {
		t.Fatalf("ReconcilePages(flat) = %v, %v", paged, err)
	}
	if _, _, err := ReconcilePages(appendHashInlineHdr(nil, 1, 0)); err == nil {
		t.Fatal("ReconcilePages accepted an inline root")
	}
	// The flat root answers through ReconcileFence, the paged one does
	// not: its fence lives in page records.
	if ids, ok := ReconcileFence(reconTestRoot(t, 7, 0)); !ok || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ReconcileFence(flat) = %v, %v", ids, ok)
	}
	if _, ok := ReconcileFence(reconTestPagedRoot(t, 600, 0)); ok {
		t.Fatal("ReconcileFence answered for a paged root")
	}
}

func TestFencePageSegids(t *testing.T) {
	page := appendHashFencePage(nil, []hashFenceEnt{
		{lo: 0, segid: 3, meta: 1},
		{lo: 1 << 20, segid: 8, meta: 0},
		{lo: 1 << 30, segid: 5, meta: 7},
	})
	ids, err := FencePageSegids(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != 3 || ids[1] != 8 || ids[2] != 5 {
		t.Fatalf("segids = %v, want [3 8 5]", ids)
	}
	if _, err := FencePageSegids([]byte{1, 0}); err == nil {
		t.Fatal("FencePageSegids accepted a short payload")
	}
}

// TestReconcileRootPaged: count and min_expire patches apply to a
// paged root the same way, and the paged bit survives the patch.
func TestReconcileRootPaged(t *testing.T) {
	root := reconTestPagedRoot(t, 600, 0)
	patched, err := ReconcileRoot(root, 5, 7000)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(patched[16:]); got != 605 {
		t.Fatalf("patched count = %d, want 605", got)
	}
	if got := int64(binary.LittleEndian.Uint64(patched[32:])); got != 7000 {
		t.Fatalf("patched min_expire = %d, want 7000", got)
	}
	sr, err := decodeHashSegRoot(patched, nil, nil)
	if err != nil {
		t.Fatalf("patched paged root fails decode: %v", err)
	}
	if !sr.paged || len(sr.pidx) != 2 {
		t.Fatalf("patch lost the page index: paged=%v pidx=%d", sr.paged, len(sr.pidx))
	}
}
