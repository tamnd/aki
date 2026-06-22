package keyspace

import (
	"fmt"
	"strings"
	"testing"
)

// TestFlushFreesTreePages proves Flush returns a multi-page tree to the freelist
// instead of orphaning it. After the flush the file holds no more pages than
// before, the freed pages sit on the freelist, and page accounting still balances.
func TestFlushFreesTreePages(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	for i := range 500 {
		if err := db.Set(fmt.Appendf(nil, "k%04d", i), []byte("v"), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)

	pagesBefore := p.PageCount()
	freeBefore := p.FreeCount()

	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit after flush: %v", err)
	}
	mustAccount(t, ks)

	if db.Len() != 0 {
		t.Fatalf("after flush Len = %d, want 0", db.Len())
	}
	if got := p.FreeCount(); got <= freeBefore {
		t.Fatalf("freelist did not grow: before %d after %d", freeBefore, got)
	}
	if got := p.PageCount(); got != pagesBefore {
		t.Fatalf("flush grew the file: before %d after %d", pagesBefore, got)
	}
}

// TestFlushFreesOverflowChains checks that the overflow pages behind a large value
// go back on the freelist when the database is flushed, the same way an overwrite
// frees the old chain.
func TestFlushFreesOverflowChains(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("big"), []byte(strings.Repeat("x", 40_000)), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set big: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)

	freeBefore := p.FreeCount()
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit after flush: %v", err)
	}
	mustAccount(t, ks)

	// The 40 KB value alone spans several overflow pages, so the freelist must
	// gain more than just the one tree page.
	if got := p.FreeCount(); got < freeBefore+5 {
		t.Fatalf("overflow chain was not freed: before %d after %d", freeBefore, got)
	}
}

// TestFlushReusesPages writes a tree, flushes it, then writes a comparable tree
// again. The second write must draw from the freelist, so the file does not keep
// growing across a flush-and-refill cycle.
func TestFlushReusesPages(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	write := func() {
		for i := range 500 {
			if err := db.Set(fmt.Appendf(nil, "k%04d", i), []byte("v"), TypeString, EncRaw, -1); err != nil {
				t.Fatalf("set %d: %v", i, err)
			}
		}
		if err := ks.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	write()
	peak := p.PageCount()
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit after flush: %v", err)
	}
	write()
	mustAccount(t, ks)

	if got := p.PageCount(); got > peak {
		t.Fatalf("refill grew the file past the peak: peak %d got %d", peak, got)
	}
}

// TestFlushDropsUsedMemory checks the used-memory estimate falls back to its
// pre-write level when a database is flushed.
func TestFlushDropsUsedMemory(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	base := ks.UsedMemory()
	for i := range 200 {
		if err := db.Set(fmt.Appendf(nil, "k%04d", i), []byte(strings.Repeat("v", 50)), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if ks.UsedMemory() <= base {
		t.Fatalf("used memory did not rise after writes: %d", ks.UsedMemory())
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := ks.UsedMemory(); got != base {
		t.Fatalf("used memory after flush = %d, want %d", got, base)
	}
}
