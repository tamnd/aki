package keyspace

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// mustAccount fails the test if the page accounting is not balanced.
func mustAccount(t *testing.T, ks *Keyspace) {
	t.Helper()
	if err := ks.CheckPageAccounting(); err != nil {
		t.Fatalf("page accounting: %v", err)
	}
}

func TestPageAccountingEmpty(t *testing.T) {
	ks, _, _ := newKS(t)
	mustAccount(t, ks)
}

func TestPageAccountingAfterWrites(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	// Enough small keys to split the tree across several pages.
	for i := range 500 {
		if err := db.Set(fmt.Appendf(nil, "k%04d", i), []byte("v"), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)
}

func TestPageAccountingWithOverflow(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	big := []byte(strings.Repeat("x", 20_000)) // spans several overflow pages
	if err := db.Set([]byte("big"), big, TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set big: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)

	// Overwriting with another large value frees the old chain and allocates a
	// new one. Accounting must stay balanced.
	if err := db.Set([]byte("big"), []byte(strings.Repeat("y", 30_000)), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("overwrite big: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)

	// Deleting it frees the chain back to the freelist.
	if _, err := db.Delete([]byte("big")); err != nil {
		t.Fatalf("delete big: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)
}

func TestPageAccountingReuseAfterDelete(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	// Create overflow pages, delete them to the freelist, then write again so the
	// allocator reuses freed pages. Accounting must hold across the reuse.
	for round := range 3 {
		key := fmt.Appendf(nil, "blob%d", round)
		if err := db.Set(key, []byte(strings.Repeat("z", 25_000)), TypeString, EncRaw, -1); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := ks.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if _, err := db.Delete(key); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := ks.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		mustAccount(t, ks)
	}
}

func TestPageAccountingAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "acct.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db := mustDB(t, ks, 0)
	for i := range 300 {
		_ = db.Set(fmt.Appendf(nil, "k%04d", i), []byte(strings.Repeat("p", 5000)), TypeString, EncRaw, -1)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustAccount(t, ks)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "acct.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	mustAccount(t, ks2)
}

// TestPageAccountingCatchesLiveOnFreelist injects a fault: it frees a page that
// is still referenced by a value's overflow chain. The check must report it.
func TestPageAccountingCatchesLiveOnFreelist(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("big"), []byte(strings.Repeat("x", 20_000)), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("set big: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	_, h, found, err := db.Peek([]byte("big"))
	if err != nil || !found {
		t.Fatalf("peek big: found %v err %v", found, err)
	}
	// Push the still-live overflow head onto the freelist behind the keyspace's
	// back. The accounting check must now object.
	if err := p.Free(uint32(h.BodyRef)); err != nil {
		t.Fatalf("free: %v", err)
	}
	if err := ks.CheckPageAccounting(); err == nil {
		t.Fatal("expected a live-and-on-freelist error, got nil")
	}
}
