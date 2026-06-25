package pager

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/vfs"
)

// Errors returned by the pager.
var (
	ErrClosed       = errors.New("aki/pager: pager is closed")
	ErrPinned       = errors.New("aki/pager: page still pinned")
	ErrInvalidPage  = errors.New("aki/pager: invalid page number")
	ErrNoMeta       = errors.New("aki/pager: no valid meta page")
	ErrReadOnlyMeta = errors.New("aki/pager: page 0/1/2 are reserved")
)

// Options configure a new or opened pager.
type Options struct {
	// PageSize is used only by Create; Open reads it from the header. Zero means
	// format.DefaultPageSize.
	PageSize uint32
	// DBCount is used only by Create. Zero means format.DefaultDBCount.
	DBCount uint32
	// CachePages is the buffer-pool capacity in frames. Zero means a default.
	CachePages int
	// CreateTimeUS stamps the header at create time. Tests pass a fixed value;
	// the engine passes the wall clock. Zero is allowed.
	CreateTimeUS uint64
}

// Pager owns a single .aki file and its in-memory page cache.
type Pager struct {
	vfs  vfs.VFS
	name string

	mu       sync.RWMutex
	file     vfs.File
	pageSize uint32
	header   format.FileHeader
	meta     format.MetaPage
	pool     *bufferPool

	// pageCount mirrors meta.PageCount as an atomic so the page-pin hot path can
	// bound-check pgno without taking p.mu (note 204). meta.PageCount stays the
	// source of truth under p.mu; every writer of it stores this mirror last, in
	// the same critical section, so a lock-free reader that sees the new bound
	// also sees a file already extended to cover it.
	pageCount atomic.Uint32

	// freelist is the in-memory stack of free page numbers; the persistent form
	// is an intrusive linked list through the free pages (doc 03 §6).
	freelist      []uint32
	freelistDirty bool

	// cacheHits and cacheMisses count buffer-pool lookups served from memory
	// versus those that had to read the page off disk. They drive the
	// aki_page_cache_hit_ratio growth field. Updated on the read path so they use
	// atomics rather than the pager mutex.
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64

	// closed is atomic so Get can check it on the hot path without p.mu. It is set
	// only by Close, after Close has confirmed zero pinned pages and the engine
	// has quiesced the write path, so a hit-path Get never races a real teardown.
	closed atomic.Bool
}

// Create initialises a fresh .aki file: page 0 header, meta pages 1 and 2, and
// an empty freelist. It fails if the file already exists with content.
func Create(fsys vfs.VFS, name string, opts Options) (*Pager, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = format.DefaultPageSize
	}
	if !format.ValidPageSize(pageSize) {
		return nil, format.ErrBadPageSize
	}
	dbCount := opts.DBCount
	if dbCount == 0 {
		dbCount = format.DefaultDBCount
	}
	f, err := fsys.Open(name, true)
	if err != nil {
		return nil, err
	}
	hdr := format.NewFileHeader(pageSize, dbCount, opts.CreateTimeUS)
	p := &Pager{
		vfs:      fsys,
		name:     name,
		file:     f,
		pageSize: pageSize,
		header:   hdr,
		pool:     newBufferPool(cacheCap(opts.CachePages)),
	}
	// Write page 0 (header), then meta A (seq 1, live) and meta B (seq 0).
	page0 := make([]byte, pageSize)
	if err := hdr.MarshalTo(page0); err != nil {
		return nil, err
	}
	metaA := format.NewMetaPage(hdr, 1)
	metaB := format.NewMetaPage(hdr, 0)
	bufA := make([]byte, pageSize)
	bufB := make([]byte, pageSize)
	if err := metaA.MarshalTo(bufA, pageSize); err != nil {
		return nil, err
	}
	if err := metaB.MarshalTo(bufB, pageSize); err != nil {
		return nil, err
	}
	if err := p.writeRaw(0, page0); err != nil {
		return nil, err
	}
	if err := p.writeRaw(format.MetaPageA, bufA); err != nil {
		return nil, err
	}
	if err := p.writeRaw(format.MetaPageB, bufB); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	p.meta = metaA
	p.pageCount.Store(metaA.PageCount)
	return p, nil
}

// Open opens an existing .aki file, validates the header, and selects the live
// meta page.
func Open(fsys vfs.VFS, name string, opts Options) (*Pager, error) {
	f, err := fsys.Open(name, false)
	if err != nil {
		return nil, err
	}
	// Read enough to parse the header; page size is not yet known, so read the
	// minimum page (4096) which always covers the 128-byte header.
	probe := make([]byte, format.MinPageSize)
	if _, err := f.ReadAt(probe, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	hdr, err := format.ParseFileHeader(probe)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !format.ValidPageSize(hdr.PageSize) {
		_ = f.Close()
		return nil, format.ErrBadPageSize
	}
	p := &Pager{
		vfs:      fsys,
		name:     name,
		file:     f,
		pageSize: hdr.PageSize,
		header:   hdr,
		pool:     newBufferPool(cacheCap(opts.CachePages)),
	}
	if err := p.loadMeta(); err != nil {
		_ = f.Close()
		return nil, err
	}
	p.pageCount.Store(p.meta.PageCount)
	if err := p.loadFreelist(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return p, nil
}

func cacheCap(n int) int {
	if n <= 0 {
		return 1024
	}
	return n
}

// PageSize returns the file's page size.
func (p *Pager) PageSize() uint32 { return p.pageSize }

// Name returns the file path this pager was opened with. It is empty for an
// in-memory backing.
func (p *Pager) Name() string { return p.name }

// PageCount returns the current total page count.
func (p *Pager) PageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta.PageCount
}

// Meta returns a copy of the live meta snapshot.
func (p *Pager) Meta() format.MetaPage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta
}

// Stats is a point-in-time snapshot of pager and buffer-pool counters. The
// server reads it for the file-growth INFO fields in doc 20 section 9.8.
type Stats struct {
	PageSize      uint32
	PageCount     uint32
	FreeCount     int
	FileBytes     int64
	ResidentPages int
	DirtyPages    int
	CacheHits     uint64
	CacheMisses   uint64
}

// CheckFreelist walks the on-disk freelist chain from the meta head, following
// the next-link in each free page. It detects a cycle and an out-of-range link,
// which a plain load cannot since it would loop or read garbage. It returns the
// number of free pages on a healthy chain. The integrity checker calls it.
func (p *Pager) CheckFreelist() (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	seen := make(map[uint32]struct{})
	head := p.meta.FreelistHead
	count := 0
	for head != format.NullPage {
		if head < 3 || head >= p.meta.PageCount {
			return count, fmt.Errorf("freelist link %d out of range (page count %d)", head, p.meta.PageCount)
		}
		if _, dup := seen[head]; dup {
			return count, fmt.Errorf("freelist cycle at page %d", head)
		}
		seen[head] = struct{}{}
		buf, err := p.readRaw(head)
		if err != nil {
			return count, err
		}
		count++
		head = encoding.U32(buf[16:])
	}
	return count, nil
}

// Stats returns the current pager counters. FileBytes is the on-disk size the
// page count implies, which is what the dataset-file growth field reports.
func (p *Pager) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	resident, dirty := p.pool.counts()
	return Stats{
		PageSize:      p.pageSize,
		PageCount:     p.meta.PageCount,
		FreeCount:     len(p.freelist),
		FileBytes:     int64(p.meta.PageCount) * int64(p.pageSize),
		ResidentPages: resident,
		DirtyPages:    dirty,
		CacheHits:     p.cacheHits.Load(),
		CacheMisses:   p.cacheMisses.Load(),
	}
}

// Header returns a copy of the file header.
func (p *Pager) Header() format.FileHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.header
}

func (p *Pager) loadMeta() error {
	bufA := make([]byte, p.pageSize)
	bufB := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(bufA, int64(format.MetaPageA)*int64(p.pageSize)); err != nil {
		return err
	}
	if _, err := p.file.ReadAt(bufB, int64(format.MetaPageB)*int64(p.pageSize)); err != nil {
		return err
	}
	mA, errA := format.ParseMetaPage(bufA)
	mB, errB := format.ParseMetaPage(bufB)
	live, ok := format.LiveMeta(mA, mB, errA == nil, errB == nil)
	if !ok {
		return ErrNoMeta
	}
	p.meta = live
	return nil
}

// writeRaw writes a full page to disk without touching the buffer pool.
func (p *Pager) writeRaw(pgno uint32, buf []byte) error {
	_, err := p.file.WriteAt(buf, int64(pgno)*int64(p.pageSize))
	return err
}

// readRaw reads a full page from disk into a fresh buffer.
func (p *Pager) readRaw(pgno uint32) ([]byte, error) {
	buf := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(buf, int64(pgno)*int64(p.pageSize)); err != nil {
		return nil, err
	}
	return buf, nil
}

// Get pins and returns the page numbered pgno, faulting it in from disk if it is
// not resident. The caller must Unpin it when done.
// Multiple goroutines may call Get concurrently; the outer RLock guards the
// closed flag and the page-count bound, and the inner pool.mu guards the frame
// table. A cache miss reads the page outside either lock so the file read runs
// in parallel with other Get calls and with the pool.mu critical section.
func (p *Pager) Get(pgno uint32) (*Page, error) {
	// The closed flag and the page-count bound are read atomically so the hit path
	// takes no shared lock here; only the owning stripe's lock is taken below, in
	// getLocked (note 204). The bound mirror is stored last by every writer that
	// grows the count, so a page number that passes the check is backed by a file
	// the writer already extended.
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if pgno >= p.pageCount.Load() {
		return nil, ErrInvalidPage
	}
	return p.getLocked(pgno)
}

func (p *Pager) getLocked(pgno uint32) (*Page, error) {
	ft := p.pool.stripe(pgno)
	// Hit path under the stripe read lock: many goroutines can pin resident pages
	// at once, since get only reads the map and the pin is an atomic add. Eviction
	// and frame inserts take the stripe write lock, so a frame cannot be dropped
	// while a hit pins it.
	ft.mu.RLock()
	if pg := ft.get(pgno); pg != nil {
		pg.pins.Add(1)
		ft.mu.RUnlock()
		p.cacheHits.Add(1)
		return pg, nil
	}
	ft.mu.RUnlock()
	p.cacheMisses.Add(1)

	buf, err := p.readRaw(pgno)
	if err != nil {
		return nil, err
	}
	pg := &Page{No: pgno, Data: buf}
	pg.ref.Store(true)
	pg.pins.Store(1)

	ft.mu.Lock()
	// Another goroutine may have loaded it meanwhile; prefer the existing frame.
	if existing := ft.get(pgno); existing != nil {
		existing.pins.Add(1)
		ft.mu.Unlock()
		return existing, nil
	}
	ft.maybeEvict()
	ft.put(pg)
	ft.mu.Unlock()
	return pg, nil
}

// Unpin releases a pin on pg. If dirty is true the page is marked for write-back
// at the next Commit.
func (p *Pager) Unpin(pg *Page, dirty bool) {
	ft := p.pool.stripe(pg.No)
	// Release under the stripe read lock: the dirty flag and the pin count are
	// atomics, so concurrent Unpins do not need to serialize. The write-back flush
	// and the eviction sweep both take the stripe write lock, so neither overlaps
	// this release.
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	if dirty {
		pg.dirty.Store(true)
	}
	for {
		n := pg.pins.Load()
		if n <= 0 {
			break
		}
		if pg.pins.CompareAndSwap(n, n-1) {
			break
		}
	}
}

// Allocate returns a fresh page for writing, reusing a freelist page when one is
// available or extending the file otherwise. The returned page is pinned and
// already marked dirty; its contents are zeroed.
func (p *Pager) Allocate() (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, ErrClosed
	}
	var pgno uint32
	if n := len(p.freelist); n > 0 {
		pgno = p.freelist[n-1]
		p.freelist = p.freelist[:n-1]
		p.freelistDirty = true
	} else {
		pgno = p.meta.PageCount
		p.meta.PageCount++
		// Publish the grown bound last so a lock-free Get that sees it also sees a
		// page whose frame this call is about to install.
		p.pageCount.Store(p.meta.PageCount)
	}
	pg := &Page{No: pgno, Data: make([]byte, p.pageSize)}
	pg.ref.Store(true)
	pg.pins.Store(1)
	pg.dirty.Store(true)
	ft := p.pool.stripe(pgno)
	ft.mu.Lock()
	// A freed-then-reallocated page may still be cached; replace it.
	if old := ft.get(pgno); old != nil {
		ft.drop(pgno)
	}
	ft.maybeEvict()
	ft.put(pg)
	ft.mu.Unlock()
	return pg, nil
}

// Free returns pgno to the freelist. Reserved pages (0, 1, 2) cannot be freed.
func (p *Pager) Free(pgno uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pgno <= format.MetaPageB {
		return ErrReadOnlyMeta
	}
	p.freelist = append(p.freelist, pgno)
	p.freelistDirty = true
	ft := p.pool.stripe(pgno)
	ft.mu.Lock()
	if pg := ft.frames[pgno]; pg != nil && pg.pins.Load() == 0 {
		ft.drop(pgno)
	}
	ft.mu.Unlock()
	return nil
}

// FreeCount returns the number of pages currently on the freelist.
func (p *Pager) FreeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.freelist)
}

// FreePages returns a copy of the in-memory freelist. The page-accounting check
// uses it to prove no live page is also free, and to find leaked pages that are
// neither live nor free.
func (p *Pager) FreePages() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]uint32, len(p.freelist))
	copy(out, p.freelist)
	return out
}

// PinnedPages returns the page numbers currently held with a non-zero pin count.
// After a command finishes every page should be unpinned, so a debug build calls
// this to catch a Get that was never matched by an Unpin (doc 23 section 9.4).
func (p *Pager) PinnedPages() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []uint32
	for i := range p.pool.stripes {
		ft := &p.pool.stripes[i]
		ft.mu.Lock()
		for pgno, pg := range ft.frames {
			if pg.pins.Load() > 0 {
				out = append(out, pgno)
			}
		}
		ft.mu.Unlock()
	}
	return out
}

// loadFreelist walks the persistent intrusive free-page chain into memory.
func (p *Pager) loadFreelist() error {
	head := p.meta.FreelistHead
	p.freelist = p.freelist[:0]
	for head != format.NullPage {
		buf, err := p.readRaw(head)
		if err != nil {
			return err
		}
		p.freelist = append(p.freelist, head)
		head = encoding.U32(buf[16:])
	}
	// The chain is read head-first; reverse so pops mirror the original order.
	for i, j := 0, len(p.freelist)-1; i < j; i, j = i+1, j-1 {
		p.freelist[i], p.freelist[j] = p.freelist[j], p.freelist[i]
	}
	return nil
}

// persistFreelist writes the intrusive chain through the free pages and returns
// the new head. Each free page stores Type=PageTypeFree and a next pointer at
// offset 16 (doc 03 §6).
func (p *Pager) persistFreelist() (uint32, error) {
	head := format.NullPage
	// Link pages so that walking from head yields reverse-insertion order; on
	// load we reverse again, restoring the stack order.
	for _, pgno := range p.freelist {
		buf := make([]byte, p.pageSize)
		h := format.PageHeader{Type: format.PageTypeFree, FreeStart: format.CommonHeaderSize, FreeEnd: uint16(p.pageSize)}
		if err := h.MarshalTo(buf); err != nil {
			return format.NullPage, err
		}
		encoding.PutU32(buf[16:], head)
		if err := p.writeRaw(pgno, buf); err != nil {
			return format.NullPage, err
		}
		head = pgno
	}
	return head, nil
}
