package sqlo1a

import (
	"context"
	"path/filepath"
	"testing"
)

// TestMintLease covers the Track A Minter: disjoint ranges, rejects
// that leave the store usable, and the mark surviving a reopen.
func TestMintLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mint.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	start, err := db.MintLease(ctx, 100)
	if err != nil || start != 0 {
		t.Fatalf("first lease: start %d, err %v", start, err)
	}
	if _, err := db.MintLease(ctx, 0); err == nil {
		t.Fatal("zero-counter lease accepted")
	}
	if _, err := db.MintLease(ctx, 1<<48); err == nil {
		t.Fatal("lease past the counter space accepted")
	}
	start, err = db.MintLease(ctx, 50)
	if err != nil || start != 100 {
		t.Fatalf("second lease: start %d, err %v, want 100", start, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	start, err = db.MintLease(ctx, 1)
	if err != nil || start != 150 {
		t.Fatalf("lease after reopen: start %d, err %v, want 150", start, err)
	}
}
