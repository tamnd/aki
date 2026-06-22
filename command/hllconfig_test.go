package command

import (
	"testing"
)

// TestHLLSparseMaxBytesConfig checks that the SPARSE-to-DENSE promotion threshold
// tracks CONFIG SET hll-sparse-max-bytes instead of a fixed constant. A small key
// stays sparse under the default, a key built while the threshold is tiny lands in
// dense, and raising the threshold again gives a fresh sparse key.
func TestHLLSparseMaxBytesConfig(t *testing.T) {
	r, c := startData(t)

	// A few elements under the default threshold stay sparse.
	if got := sendLine(t, r, c, "PFADD hk a b c"); got != ":1" {
		t.Fatalf("PFADD hk = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hk"); got != "+sparse" {
		t.Fatalf("encoding hk = %q want sparse", got)
	}

	// Drop the threshold so any non-empty body is forced to dense.
	if got := sendLine(t, r, c, "CONFIG SET hll-sparse-max-bytes 1"); got != "+OK" {
		t.Fatalf("CONFIG SET hll-sparse-max-bytes 1 = %q", got)
	}

	// A new key built under the tiny threshold lands in dense right away.
	if got := sendLine(t, r, c, "PFADD hk2 x"); got != ":1" {
		t.Fatalf("PFADD hk2 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hk2"); got != "+dense" {
		t.Fatalf("encoding hk2 = %q want dense", got)
	}

	// The first key was not written again, so it keeps its sparse encoding:
	// promotion happens on update, not retroactively.
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hk"); got != "+sparse" {
		t.Fatalf("encoding hk after lower threshold = %q want sparse", got)
	}

	// Raising the threshold again lets a fresh small key stay sparse.
	if got := sendLine(t, r, c, "CONFIG SET hll-sparse-max-bytes 3000"); got != "+OK" {
		t.Fatalf("CONFIG SET hll-sparse-max-bytes 3000 = %q", got)
	}
	if got := sendLine(t, r, c, "PFADD hk3 x"); got != ":1" {
		t.Fatalf("PFADD hk3 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PFDEBUG ENCODING hk3"); got != "+sparse" {
		t.Fatalf("encoding hk3 = %q want sparse", got)
	}
}
