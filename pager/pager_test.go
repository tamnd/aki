package pager

import (
	"testing"

	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/vfs"
)

func newTestPager(t *testing.T) (*Pager, vfs.VFS) {
	t.Helper()
	fsys := vfs.NewMem()
	p, err := Create(fsys, "test.aki", Options{CreateTimeUS: 123})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return p, fsys
}

func TestCreateInitialState(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()
	if p.PageSize() != format.DefaultPageSize {
		t.Errorf("page size %d", p.PageSize())
	}
	if p.PageCount() != 3 {
		t.Errorf("page count %d want 3", p.PageCount())
	}
	m := p.Meta()
	if m.MetaSeq != 1 {
		t.Errorf("initial meta seq %d want 1", m.MetaSeq)
	}
	if m.CatalogRoot != format.NullPage {
		t.Errorf("catalog root %#x want null", m.CatalogRoot)
	}
}

func TestAllocateWriteCommitReopen(t *testing.T) {
	p, fsys := newTestPager(t)
	pg, err := p.Allocate()
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	pgno := pg.No
	if pgno != 3 {
		t.Errorf("first allocated page %d want 3", pgno)
	}
	copy(pg.Data, []byte("payload-A"))
	p.Unpin(pg, true)

	var roots [8]uint32
	roots[0] = pgno
	if err := p.Commit(CommitInfo{SetDBRoots: true, DBRootPages: roots}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the page and meta persisted.
	p2, err := Open(fsys, "test.aki", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p2.Close()
	if p2.Meta().MetaSeq != 2 {
		t.Errorf("reopened meta seq %d want 2", p2.Meta().MetaSeq)
	}
	if p2.Meta().DBRootPages[0] != pgno {
		t.Errorf("db root not persisted: %d", p2.Meta().DBRootPages[0])
	}
	got, err := p2.Get(pgno)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer p2.Unpin(got, false)
	if string(got.Data[:9]) != "payload-A" {
		t.Errorf("page data %q", got.Data[:9])
	}
}

func TestFreeAndReuse(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()
	a, _ := p.Allocate()
	b, _ := p.Allocate()
	an, bn := a.No, b.No
	p.Unpin(a, true)
	p.Unpin(b, true)
	if err := p.Commit(CommitInfo{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Free(bn); err != nil {
		t.Fatal(err)
	}
	if p.FreeCount() != 1 {
		t.Errorf("free count %d want 1", p.FreeCount())
	}
	// Next allocation reuses the freed page.
	c, _ := p.Allocate()
	if c.No != bn {
		t.Errorf("reused page %d want %d", c.No, bn)
	}
	p.Unpin(c, true)
	_ = an
}

func TestFreelistPersistsAcrossReopen(t *testing.T) {
	fsys := vfs.NewMem()
	p, _ := Create(fsys, "f.aki", Options{})
	a, _ := p.Allocate()
	b, _ := p.Allocate()
	cpg, _ := p.Allocate()
	p.Unpin(a, true)
	p.Unpin(b, true)
	p.Unpin(cpg, true)
	p.Commit(CommitInfo{})
	p.Free(a.No)
	p.Free(cpg.No)
	freed := p.FreeCount()
	p.Commit(CommitInfo{})
	p.Close()

	p2, err := Open(fsys, "f.aki", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if p2.FreeCount() != freed {
		t.Errorf("freelist after reopen %d want %d", p2.FreeCount(), freed)
	}
}

func TestReservedPagesCannotBeFreed(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()
	for _, pgno := range []uint32{0, 1, 2} {
		if err := p.Free(pgno); err != ErrReadOnlyMeta {
			t.Errorf("Free(%d)=%v want ErrReadOnlyMeta", pgno, err)
		}
	}
}

func TestCommitAlternatesMetaPages(t *testing.T) {
	p, fsys := newTestPager(t)
	for i := range 5 {
		pg, _ := p.Allocate()
		p.Unpin(pg, true)
		if err := p.Commit(CommitInfo{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	wantSeq := uint64(1 + 5)
	if p.Meta().MetaSeq != wantSeq {
		t.Errorf("meta seq %d want %d", p.Meta().MetaSeq, wantSeq)
	}
	p.Close()
	// Reopen picks the highest-seq meta.
	p2, _ := Open(fsys, "test.aki", Options{})
	defer p2.Close()
	if p2.Meta().MetaSeq != wantSeq {
		t.Errorf("reopened seq %d want %d", p2.Meta().MetaSeq, wantSeq)
	}
}

func TestGetInvalidPage(t *testing.T) {
	p, _ := newTestPager(t)
	defer p.Close()
	if _, err := p.Get(999); err != ErrInvalidPage {
		t.Errorf("got %v want ErrInvalidPage", err)
	}
}

func TestClosePinnedFails(t *testing.T) {
	p, _ := newTestPager(t)
	pg, _ := p.Allocate()
	if err := p.Close(); err != ErrPinned {
		t.Errorf("Close with pinned page=%v want ErrPinned", err)
	}
	p.Unpin(pg, false)
	if err := p.Close(); err != nil {
		t.Errorf("Close after unpin: %v", err)
	}
}

func TestBufferPoolEviction(t *testing.T) {
	fsys := vfs.NewMem()
	// Tiny cache to force eviction.
	p, _ := Create(fsys, "e.aki", Options{CachePages: 8})
	// Allocate and commit many pages.
	var nums []uint32
	for range 40 {
		pg, _ := p.Allocate()
		pg.Data[0] = byte(pg.No)
		nums = append(nums, pg.No)
		p.Unpin(pg, true)
	}
	if err := p.Commit(CommitInfo{}); err != nil {
		t.Fatal(err)
	}
	// Read them all back; eviction must not lose data since it is committed.
	for _, n := range nums {
		pg, err := p.Get(n)
		if err != nil {
			t.Fatalf("Get(%d): %v", n, err)
		}
		if pg.Data[0] != byte(n) {
			t.Errorf("page %d data[0]=%d", n, pg.Data[0])
		}
		p.Unpin(pg, false)
	}
	p.Close()
}

func TestCrashBeforeCommitRollsBack(t *testing.T) {
	fsys := vfs.NewMem()
	p, _ := Create(fsys, "c.aki", Options{})
	pg, _ := p.Allocate()
	copy(pg.Data, []byte("committed"))
	p.Unpin(pg, true)
	p.Commit(CommitInfo{})
	committedSeq := p.Meta().MetaSeq

	// Make a dirty change but do NOT commit; simulate crash by abandoning.
	pg2, _ := p.Allocate()
	copy(pg2.Data, []byte("uncommitted"))
	p.Unpin(pg2, true)
	// no Commit
	p.Close()

	p2, _ := Open(fsys, "c.aki", Options{})
	defer p2.Close()
	if p2.Meta().MetaSeq != committedSeq {
		t.Errorf("seq after crash %d want %d (rollback)", p2.Meta().MetaSeq, committedSeq)
	}
	if p2.PageCount() != 4 { // 3 reserved + 1 committed page
		t.Errorf("page count %d want 4", p2.PageCount())
	}
}
