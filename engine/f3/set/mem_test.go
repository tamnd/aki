package set

import (
	"strconv"
	"testing"
	"unsafe"
)

// TestBytesPerMember accounts the native band's per-member overhead against the
// doc 11 section 11.1 ledger and the PRED-F3-M1-SETMEM target. It fills the
// table to exactly 7/8 load so the bucket term reproduces the lab-01 figure
// (5.71 bytes) rather than a power-of-two rounding artifact, then sums the three
// overhead terms the way the doc's ledger does:
//
//	bucket  = table slots * 5 bytes (1 control + 4 ordinal), divided by members
//	record  = sizeof(record), the fixed per-member cell
//	vector  = 4 bytes, one uint32 ordinal in the dense draw vector
//
// Member bytes are excluded (they are the payload, not overhead). The diet steps
// are already baked in: the record caches no hash32 (step one) and no tag (step
// two, the tag lives in the control byte), which is what lands the total in the
// ~21-23 band the doc predicts, under the ~26-28 baseline and inside range of
// Valkey 8.1's 10-20 byte embedded-entry bar.
func TestBytesPerMember(t *testing.T) {
	// 14336 = 16384 * 7/8, so the table settles at cap 16384 exactly full.
	const members = 14336
	const wantCap = 16384

	s := newSet([]byte("0"))
	for i := 0; i < members; i++ {
		s.add([]byte("member:" + strconv.Itoa(i)))
	}
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	h := s.ht
	if got := h.tbl.CapSlots(); got != wantCap {
		t.Fatalf("table cap = %d, want %d (load not 7/8, ledger would misreport)", got, wantCap)
	}

	const recordBytes = float64(unsafe.Sizeof(record{}))
	if recordBytes != 12 {
		t.Fatalf("record is %.0f bytes, want 12 (diet layout: loc, vslot, mlen, flags)", recordBytes)
	}

	bucket := float64(h.tbl.CapSlots()) * 5 / float64(members)
	vector := 4.0
	total := bucket + recordBytes + vector

	t.Logf("native bytes/member: bucket %.2f + record %.0f + vector %.0f = %.2f",
		bucket, recordBytes, vector, total)
	t.Logf("  baseline ~26-28 (with hash32+tag) | target ~21-23 (diet) | Valkey 8.1 embedded 10-20")

	if bucket < 5.6 || bucket > 5.8 {
		t.Errorf("bucket term = %.2f, want ~5.71 at 7/8 load", bucket)
	}
	if total < 21 || total > 23 {
		t.Errorf("bytes/member = %.2f, outside the PRED-F3-M1-SETMEM 21-23 target", total)
	}
}
