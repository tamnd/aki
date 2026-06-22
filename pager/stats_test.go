package pager

import "testing"

func TestStats(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()

	// A fresh file has the header and two meta pages, so the size the stats report
	// is the page count times the page size.
	st := p.Stats()
	if st.PageSize != p.PageSize() {
		t.Errorf("PageSize = %d, want %d", st.PageSize, p.PageSize())
	}
	if st.PageCount != p.PageCount() {
		t.Errorf("PageCount = %d, want %d", st.PageCount, p.PageCount())
	}
	if want := int64(p.PageCount()) * int64(p.PageSize()); st.FileBytes != want {
		t.Errorf("FileBytes = %d, want %d", st.FileBytes, want)
	}

	// Allocate a page, commit, then read it twice. The first read after the page
	// fell out of cache is a miss; a repeat read is a hit.
	pg, err := p.Allocate()
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	pgno := pg.No
	p.Unpin(pg, true)
	if err := p.Commit(CommitInfo{}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	before := p.Stats()
	got, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Unpin(got, false)
	got2, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	p.Unpin(got2, false)

	after := p.Stats()
	if after.CacheHits <= before.CacheHits {
		t.Errorf("CacheHits did not grow: before %d after %d", before.CacheHits, after.CacheHits)
	}
	if after.ResidentPages == 0 {
		t.Errorf("ResidentPages = 0 after reads, want non-zero")
	}
}
