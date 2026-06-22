package pager

import "testing"

func TestCheckFreelist(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()

	// A fresh file has an empty freelist.
	n, err := p.CheckFreelist()
	if err != nil {
		t.Fatalf("CheckFreelist on fresh file: %v", err)
	}
	if n != 0 {
		t.Fatalf("fresh freelist count = %d, want 0", n)
	}

	// Allocate three pages and commit so they exist on disk.
	var pgnos []uint32
	for range 3 {
		pg, aerr := p.Allocate()
		if aerr != nil {
			t.Fatalf("Allocate: %v", aerr)
		}
		pgnos = append(pgnos, pg.No)
		p.Unpin(pg, true)
	}
	if err := p.Commit(CommitInfo{}); err != nil {
		t.Fatalf("Commit after allocate: %v", err)
	}

	// Free them all and commit so the chain is persisted.
	for _, pgno := range pgnos {
		if ferr := p.Free(pgno); ferr != nil {
			t.Fatalf("Free(%d): %v", pgno, ferr)
		}
	}
	if err := p.Commit(CommitInfo{}); err != nil {
		t.Fatalf("Commit after free: %v", err)
	}

	n, err = p.CheckFreelist()
	if err != nil {
		t.Fatalf("CheckFreelist after free: %v", err)
	}
	if n != p.FreeCount() {
		t.Errorf("CheckFreelist count = %d, FreeCount = %d, want equal", n, p.FreeCount())
	}
	if n != len(pgnos) {
		t.Errorf("CheckFreelist count = %d, want %d", n, len(pgnos))
	}
}
